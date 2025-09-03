# services/schema_migrator.py
import logging
import time
import threading
import multiprocessing
from typing import Dict, Any, List, Optional, Tuple, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass


from services.db2_service import (
    improved_table_migration_with_monitoring,
    create_schema_if_not_exists,
    create_tables_multithreaded,
    get_table_row_count,
    cleanup_connections,
)
from utils.couchdb_helpers import save_migration_status_to_couchdb
from services.oracle_service import fetch_tables as fetch_oracle_tables, fetch_table_metadata as fetch_oracle_metadata
from services.sql_service import fetch_tables as fetch_sql_tables, fetch_table_metadata as fetch_sql_metadata
from utils.ddl_writer import save_ddl


logger = logging.getLogger(__name__)



@dataclass
class MigrationConfig:
    table_creation_workers: int = min(12, multiprocessing.cpu_count())
    data_migration_workers: int = min(12, multiprocessing.cpu_count())
    batch_size: int = 1000
    enable_validation: bool = True
    max_retries: int = 3


@dataclass
class TableMigrationResult:
    table: str
    status: str  # 'success', 'failed', 'skipped'
    rows_migrated: int = 0
    duration: float = 0.0
    error: Optional[str] = None
    verified: bool = False


@dataclass
class SchemaMigrationResult:
    source_schema: str
    target_schema: str
    total_tables: int
    successful_migrations: int
    failed_migrations: int
    total_rows_migrated: int
    total_duration: float
    table_results: List[TableMigrationResult]


    @property
    def success_rate(self) -> float:
        if self.total_tables > 0:
            return (self.successful_migrations / self.total_tables) * 100
        return 0.0



class MigrationStatus:
    def __init__(self, schema_name: str):
        self.schema = schema_name
        self.status = {
            'tables': {'success': [], 'error': []},
            'triggers': {'success': [], 'error': []},
            'sequences': {'success': [], 'error': []},
            'indexes': {'success': [], 'error': []},
            'views': {'success': [], 'error': []},
        }


    def store_success(self, obj_type: str, obj_name: str):
        if obj_name not in self.status[obj_type]['success']:
            self.status[obj_type]['success'].append(obj_name)
        if obj_name in self.status[obj_type]['error']:
            self.status[obj_type]['error'].remove(obj_name)


    def store_error(self, obj_type: str, obj_name: str):
        if obj_name not in self.status[obj_type]['error']:
            self.status[obj_type]['error'].append(obj_name)
        if obj_name in self.status[obj_type]['success']:
            self.status[obj_type]['success'].remove(obj_name)



class OptimizedSchemaMigrator:
    def __init__(self, config: Optional[MigrationConfig] = None):
        self.config = config or MigrationConfig()
        self._stats_lock = threading.Lock()
        self._migration_stats = {"total_rows": 0, "total_duration": 0}


    def migrate_schema_fast(
        self,
        source_type: str,
        source_schema: str,
        target_schema: Optional[str] = None,
        table_filter: Optional[List[str]] = None,
    ) -> SchemaMigrationResult:
        target_schema = target_schema or source_schema
        start_time = time.time()


        self._validate_schemas(source_type, source_schema, target_schema)


        all_tables, metadata_map = self._get_all_metadata_parallel(
            source_type, source_schema, table_filter
        )


        self._create_tables_without_verification(target_schema, metadata_map, source_type)


        migration_results = self._migrate_data_parallel(
            source_type, source_schema, target_schema, all_tables
        )


        if self.config.enable_validation:
            self._verify_migration_parallel(target_schema, migration_results)


        total_duration = time.time() - start_time
        self._migration_stats["total_duration"] = total_duration
        self._migration_stats["total_rows"] = sum(
            r.rows_migrated for r in migration_results.values()
        )


        result = self._compile_results(
            source_schema, target_schema, all_tables, migration_results, start_time
        )
        self._log_migration_summary(result)
        return result


    #--------------- NEW: streaming generator for migration SSE --------------
    def migrate_schema_streaming(
        self,
        source_type: str,
        source_schema: str,
        target_schema: Optional[str] = None,
        table_filter: Optional[List[str]] = None,
        transaction_id: Optional[str] = None,
    ) -> Generator[str, None, None]:
        target_schema = target_schema or source_schema
        migration_status = MigrationStatus(source_schema)
        try:
            yield f"🚀 Starting migration: {source_schema} → {target_schema}"
            yield "🔍 Validating schemas..."
            self._validate_schemas(source_type, source_schema, target_schema)
            yield "✅ Schemas validated"
            yield "📋 Fetching metadata..."
            all_tables, metadata_map = self._get_all_metadata_parallel(source_type, source_schema, table_filter)
            yield f"✅ Metadata fetched for {len(all_tables)} tables"
            yield "🔨 Creating tables ..."
            self._create_tables_without_verification(target_schema, metadata_map, source_type)
            yield "✅ Tables created"
            yield "📊 Migrating data..."


            results = {}


            def migrate_table(table: str):
                return self._migrate_data_parallel(
                    source_type, source_schema, target_schema, [table],
                    migration_status=migration_status,
                    transaction_id=transaction_id
                )[table]


            with ThreadPoolExecutor(max_workers=self.config.data_migration_workers) as executor:
                futures = {executor.submit(migrate_table, t): t for t in all_tables}
                for future in as_completed(futures):
                    table = futures[future]
                    result = future.result()
                    results[table] = result
                    yield f"✅ {table}: {result.status} - rows migrated: {result.rows_migrated}" if result.status == "success" else f"❌ {table}: {result.error}"
                    if transaction_id:
                        save_migration_status_to_couchdb(transaction_id, migration_status.status, migration_status.schema)
                    progress = len(results) / len(all_tables) * 100
                    yield f"📈 Progress: {progress:.1f}% ({len(results)}/{len(all_tables)})"


            success_count = sum(1 for r in results.values() if r.status == "success")
            failed_count = sum(1 for r in results.values() if r.status == "failed")
            yield f"🎉 Migration finished: {success_count} success / {failed_count} failed"


        except Exception as e:
            yield f"❌ Migration failed with error: {e}"
            logger.error(f"Migration stream error: {e}")
        finally:
            cleanup_connections()


    #------------------ rest of your methods below, unchanged ----------------


    def _create_tables_without_verification(
        self,
        target_schema: str,
        metadata_map: Dict[str, List[Dict]],
        source_type: str,
    ) -> Dict[str, str]:
        logger.info("🔨 Creating tables in bulk ...")
        creation_results = create_tables_multithreaded(
            target_schema,
            [(table, metadata, source_type) for table, metadata in metadata_map.items()],
            max_workers=self.config.table_creation_workers,
        )
        logger.info(f"🏁 Table creation complete: {len(creation_results)} tables")
        return creation_results


    def _validate_schemas(
        self,
        source_type: str,
        source_schema: str,
        target_schema: str,
    ):
        logger.info("🔍 Validating schemas...")
        if source_type.lower() == "oracle":
            test_tables = fetch_oracle_tables(source_schema)
        else:
            test_tables = fetch_sql_tables(source_schema)
        if not test_tables:
            raise ValueError(f"Source schema '{source_schema}' is empty or inaccessible (no MAXOBJECT tables)")
        if not create_schema_if_not_exists(target_schema):
            raise ValueError(f"Cannot create/access target schema '{target_schema}'")
        logger.info(f"✅ Schemas validated - Source: {len(test_tables)} tables")


    def _get_all_metadata_parallel(
        self,
        source_type: str,
        source_schema: str,
        table_filter: Optional[List[str]] = None,
    ) -> Tuple[List[str], Dict[str, List[Dict]]]:
        logger.info("📋 Fetching metadata in parallel...")


        if source_type.lower() == "oracle":
            maxobject_tables = fetch_oracle_tables(source_schema)
            fetch_metadata = fetch_oracle_metadata
        else:
            maxobject_tables = fetch_sql_tables(source_schema)
            fetch_metadata = fetch_sql_metadata


        maxobject_tables_upper = set(t.upper() for t in maxobject_tables)


        if table_filter is None:
            filtered_tables = maxobject_tables_upper
        else:
            table_filter_upper = set(t.upper() for t in table_filter)
            filtered_tables = maxobject_tables_upper.intersection(table_filter_upper)


        if not filtered_tables:
            logger.warning("⚠️ No tables to fetch metadata for after applying MAXOBJECT filter and table_filter.")
            return [], {}


        all_tables_upper = list(filtered_tables)
        metadata_map = {}


        def fetch_table_metadata(table):
            try:
                metadata = fetch_metadata(source_schema, table)
                print(metadata)
                return table, metadata
            except Exception as e:
                logger.warning(f"⚠️ Metadata fetch failed for {table}: {e}")
                return table, None
        
        
        def fetch_table_metadata(table):
            try:
                metadata = fetch_metadata(source_schema, table)
                ddl = metadata.get("ddl") if isinstance(metadata, dict) else None
                if ddl:
                    save_ddl("source", source_schema, table, ddl, object_type="table")
                return table, metadata
            except Exception as e:
                logger.warning(f"⚠️ Metadata fetch failed for {table}: {e}")
                return table, None


        with ThreadPoolExecutor(max_workers=self.config.table_creation_workers) as executor:
            futures = {executor.submit(fetch_table_metadata, table): table for table in all_tables_upper}
            for future in as_completed(futures):
                table, metadata = future.result()
                if metadata:
                    metadata_map[table] = metadata
                else:
                    logger.warning(f"⚠️ Skipping {table} - no metadata")


        valid_tables = list(metadata_map.keys())
        
        logger.info(f"✅ Metadata fetched for {len(valid_tables)} tables")
        return valid_tables, metadata_map


    def _migrate_data_parallel(
        self,
        source_type: str,
        source_schema: str,
        target_schema: str,
        tables: List[str],
        migration_status: Optional[MigrationStatus] = None,
        transaction_id: Optional[str] = None,
    ) -> Dict[str, TableMigrationResult]:
        logger.info(f"📊 Migrating data with {self.config.data_migration_workers} workers...")
        results = {}


        def migrate_single_table_with_retries(table: str) -> TableMigrationResult:
            last_error = None
            for attempt in range(1, self.config.max_retries + 1):
                start_time = time.time()
                try:
                    raw_result = improved_table_migration_with_monitoring(
                        source_type, source_schema, target_schema, table, timeout_minutes=15
                    )
                    if raw_result.get("status") == "success":
                        duration = time.time() - start_time
                        if migration_status is not None:
                            migration_status.store_success("tables", table)
                        if transaction_id:
                            save_migration_status_to_couchdb(
                                transaction_id, migration_status.status, migration_status.schema
                            )
                        return TableMigrationResult(
                            table=table,
                            status="success",
                            rows_migrated=raw_result.get("rows_migrated", 0),
                            duration=duration,
                        )
                    else:
                        last_error = raw_result.get("error")
                        logger.error(f"[{table}] Attempt {attempt}/{self.config.max_retries} failed: {last_error}")
                except Exception as e:
                    last_error = str(e)
                    logger.error(f"[{table}] Exception on attempt {attempt}/{self.config.max_retries}: {last_error}")


                if attempt < self.config.max_retries:
                    time.sleep(3)


            if migration_status is not None:
                migration_status.store_error("tables", table)
            if transaction_id:
                save_migration_status_to_couchdb(transaction_id, migration_status.status, migration_status.schema)


            return TableMigrationResult(
                table=table,
                status="failed",
                error=last_error,
                rows_migrated=0,
                duration=0.0,
            )


        with ThreadPoolExecutor(max_workers=self.config.data_migration_workers) as executor:
            futures = {executor.submit(migrate_single_table_with_retries, table): table for table in tables}
            for future in as_completed(futures):
                table = futures[future]
                try:
                    result = future.result()
                    results[table] = result
                    completed = len(results)
                    progress = (completed / len(tables)) * 100
                    logger.info(f"📈 Progress: {progress:.1f}% ({completed}/{len(tables)})")
                except Exception as e:
                    logger.error(f"❌ Migration task failed for {table}: {e}")
                    results[table] = TableMigrationResult(table=table, status="failed", error=str(e))
                    if migration_status is not None:
                        migration_status.store_error("tables", table)
                        if transaction_id:
                            save_migration_status_to_couchdb(transaction_id, migration_status.status, migration_status.schema)
        return results


    def _verify_migration_parallel(
        self,
        target_schema: str,
        migration_results: Dict[str, TableMigrationResult],
    ):
        if not self.config.enable_validation:
            return
        logger.info("🔍 Verifying migration...")


        def verify_table(table: str, result: TableMigrationResult):
            try:
                if result.status != "success":
                    return
                target_count = get_table_row_count(target_schema, table)
                if target_count == result.rows_migrated:
                    result.verified = True
                    logger.debug(f"✅ {table}: Verified {target_count:,} rows")
                else:
                    result.verified = False
                    logger.warning(
                        f"⚠️ {table}: Count mismatch - expected {result.rows_migrated:,}, got {target_count:,}"
                    )
            except Exception as e:
                logger.warning(f"⚠️ Verification failed for {table}: {e}")
                result.verified = False


        with ThreadPoolExecutor(max_workers=self.config.table_creation_workers) as executor:
            futures = []
            for table, result in migration_results.items():
                if result.status == "success":
                    futures.append(executor.submit(verify_table, table, result))
            for future in as_completed(futures):
                future.result()
        verified_count = sum(1 for r in migration_results.values() if r.verified)
        total_successful = sum(1 for r in migration_results.values() if r.status == "success")
        logger.info(f"✅ Verification: {verified_count}/{total_successful} tables verified")


    def _compile_results(
        self,
        source_schema: str,
        target_schema: str,
        all_tables: List[str],
        migration_results: Dict[str, TableMigrationResult],
        start_time: float,
    ) -> SchemaMigrationResult:
        successful = sum(1 for r in migration_results.values() if r.status == "success")
        failed = sum(1 for r in migration_results.values() if r.status == "failed")
        return SchemaMigrationResult(
            source_schema=source_schema,
            target_schema=target_schema,
            total_tables=len(all_tables),
            successful_migrations=successful,
            failed_migrations=failed,
            total_rows_migrated=self._migration_stats["total_rows"],
            total_duration=time.time() - start_time,
            table_results=list(migration_results.values()),
        )


    def _log_migration_summary(self, result: SchemaMigrationResult):
        logger.info("=" * 80)
        logger.info("🎉 MIGRATION COMPLETED")
        logger.info("=" * 80)
        logger.info(f"📊 Schema: {result.source_schema} → {result.target_schema}")
        logger.info(f"⏱️ Duration: {result.total_duration:.2f} seconds")
        logger.info(
            f"📈 Success: {result.successful_migrations}/{result.total_tables} tables ({result.success_rate:.1f}%)"
        )
        logger.info(f"📋 Rows: {result.total_rows_migrated:,} total")
        logger.info(
            f"⚡ Rate: {result.total_rows_migrated/result.total_duration:.0f} rows/second"
        )
        if result.failed_migrations > 0:
            logger.warning(f"⚠️ Failed tables ({result.failed_migrations}):")
            for table_result in result.table_results:
                if table_result.status == "failed":
                    logger.warning(f"  - {table_result.table}: {table_result.error}")
        logger.info("=" * 80)


def migrate_schema_fast(
    source_type: str,
    source_schema: str,
    target_schema: Optional[str] = None,
    table_filter: Optional[List[str]] = None,
    config: Optional[MigrationConfig] = None,
) -> SchemaMigrationResult:
    config = config or MigrationConfig()
    migrator = OptimizedSchemaMigrator(config)
    return migrator.migrate_schema_fast(
        source_type=source_type,
        source_schema=source_schema,
        target_schema=target_schema,
        table_filter=table_filter,
    )
