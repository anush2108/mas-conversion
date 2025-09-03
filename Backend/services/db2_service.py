# services/db2_service.py 
import datetime
import json
import logging
import ibm_db
import threading

from decimal import Decimal
from typing import List, Dict, Optional, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import queue
import os
import multiprocessing
from ibm_db_dbi import Connection as DBI_Connection

from utils.credentials_store import load_credentials
from utils.oracle_type_mapper import oracle_to_db2_type
from utils.sql_type_mapper import sql_to_db2_type

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────── GLOBAL OPTIMIZATION CACHE ───────────────────────
_connection_cache = {}
_metadata_cache = {}
_ddl_cache = {}
_type_cache = {}
_cache_lock = threading.Lock()

# ─────────────────────── ULTRA-FAST CONNECTION POOL ───────────────────────
def get_db2_connection_fast():
    thread_id = threading.get_ident()
    with _cache_lock:
        if thread_id in _connection_cache:
            conn = _connection_cache[thread_id]
            try:
                ibm_db.exec_immediate(conn, "SELECT 1 FROM SYSIBM.SYSDUMMY1")
                return conn
            except:
                del _connection_cache[thread_id]
    try:
        creds = load_credentials("db2", is_target=True)
        dsn = (
            f"DATABASE={creds['database']};HOSTNAME={creds['host']};PORT={creds['port']};"
            f"UID={creds['username']};PWD={creds['password']};SECURITY={creds.get('security', 'SSL')};"
            f"CHARSET=UTF-8;AUTOCOMMIT=0;CONNECTTIMEOUT=30;QUERYTIMEOUT=300;CURRENTSCHEMA={creds.get('schema', 'DB2ADMIN')};"
        )
        conn = ibm_db.connect(dsn, "", "")
        ibm_db.set_option(conn, {
            ibm_db.SQL_ATTR_AUTOCOMMIT: ibm_db.SQL_AUTOCOMMIT_OFF,
            ibm_db.SQL_ATTR_TXN_ISOLATION: ibm_db.SQL_TXN_READ_COMMITTED
        }, 1)
        with _cache_lock:
            _connection_cache[thread_id] = conn
        return conn
    except Exception as e:
        logger.error(f"DB2 connection error: {e}")
        raise
    
get_db2_connection = get_db2_connection_fast

def close_all_connections():
    with _cache_lock:
        for conn in _connection_cache.values():
            try:
                ibm_db.close(conn)
            except:
                pass
        _connection_cache.clear()

# ─────────────────────── CACHED SCHEMA OPERATIONS ───────────────────────
def create_schema_if_not_exists(schema_name: str) -> bool:
    """Create schema if it doesn't exist - with caching"""
    schema_upper = schema_name.upper()
    
    with _cache_lock:
        if schema_upper in _ddl_cache:
            return _ddl_cache[schema_upper]
    
    conn = get_db2_connection_fast()
    try:
        # Check if schema exists
        check_stmt = f"SELECT 1 FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = '{schema_upper}'"
        result = ibm_db.exec_immediate(conn, check_stmt)
        
        if result and ibm_db.fetch_assoc(result):
            with _cache_lock:
                _ddl_cache[schema_upper] = True
            return True

        # Create schema
        create_stmt = f'CREATE SCHEMA "{schema_upper}"'
        result = ibm_db.exec_immediate(conn, create_stmt)
        
        if result:
            ibm_db.commit(conn)
            with _cache_lock:
                _ddl_cache[schema_upper] = True
            return True
        else:
            ibm_db.rollback(conn)
            return False
            
    except Exception as e:
        logger.error(f"Schema error {schema_name}: {e}")
        return False


_ddl_cache = {}
_cache_lock = threading.Lock()

def clear_table_cache():
    """Clear the in-memory table existence cache"""
    with _cache_lock:
        _ddl_cache.clear()
        print("🧹 Table existence cache cleared")

def check_table_exists(schema: str, table: str, skip_cache: bool = False) -> bool:
    """
    Checks if a table exists in DB2 under the given schema using SYSCAT.TABLES.
    Uses in-memory cache for faster subsequent lookups unless skip_cache is True.
    """
    schema_upper = schema.upper()
    table_upper = table.upper()
    cache_key = f"{schema_upper}.{table_upper}"

    # Check in-memory cache first (unless skipping cache)
    if not skip_cache:
        with _cache_lock:
            if cache_key in _ddl_cache:
                return _ddl_cache[cache_key]

    try:
        conn = get_db2_connection_fast()
        query = f"""
            SELECT 1 
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA = '{schema_upper}' AND TABNAME = '{table_upper}'
        """
        result = ibm_db.exec_immediate(conn, query)
        exists = bool(result and ibm_db.fetch_assoc(result))

        # Save to cache
        with _cache_lock:
            _ddl_cache[cache_key] = exists

        if not exists:
            print(f"❌ Missing in DB2: {schema_upper}.{table_upper}")
        else:
            print(f"✅ Found in DB2: {schema_upper}.{table_upper}")

        return exists

    except Exception as e:
        print(f"[❌ Error checking table]: {schema}.{table} — {e}")
        return False


def get_table_column_info_cached(schema: str, table: str) -> List[Dict[str, Any]]:
    """Get column info with caching"""
    cache_key = f"cols_{schema.upper()}.{table.upper()}"
    
    with _cache_lock:
        if cache_key in _metadata_cache:
            return _metadata_cache[cache_key]
    
    conn = get_db2_connection_fast()
    query = f"""
        SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS
        FROM SYSCAT.COLUMNS
        WHERE TABSCHEMA = '{schema.upper()}' AND TABNAME = '{table.upper()}'
        ORDER BY COLNO
    """
    result = ibm_db.exec_immediate(conn, query)
    columns = []
    
    while row := ibm_db.fetch_assoc(result):
        columns.append({
            'column_name': row['COLNAME'],
            'data_type': row['TYPENAME'],
            'length': row['LENGTH'],
            'scale': row['SCALE'],
            'nullable': row['NULLS']
        })
    
    with _cache_lock:
        _metadata_cache[cache_key] = columns
    
    return columns

# ─────────────────────── ENHANCED VALUE SANITIZER WITH LOB SUPPORT ───────────────────────
def safe_lob_read(lob_obj, max_size=1048576):  # 1MB limit
    """Safely read LOB data with size limits"""
    try:
        if hasattr(lob_obj, 'read'):
            return lob_obj.read()
        elif hasattr(lob_obj, 'size') and hasattr(lob_obj, 'read'):
            size = min(lob_obj.size(), max_size)
            return lob_obj.read(size)
        else:
            return str(lob_obj)
    except Exception as e:
        logger.warning(f"LOB read failed: {e}")
        return f"<LOB_READ_ERROR: {type(lob_obj).__name__}>"

def improved_sanitize_value(val: Any, target_type: str, max_length: int = None) -> Any:
    """Enhanced value sanitizer with proper LOB and bytes handling"""
    if val is None:
        return None
    
    try:
        # Handle Oracle LOB objects first
        if hasattr(val, 'read') or str(type(val)).lower().find('lob') != -1:
            try:
                val = safe_lob_read(val)
            except Exception as e:
                logger.warning(f"LOB conversion failed: {e}")
                return None
        
        # Handle bytes objects
        if isinstance(val, bytes):
            try:
                val = val.decode('utf-8', errors='replace')
            except Exception:
                val = str(val)[2:-1]  # Remove b' and '
        
        # Handle other object types that might cause __str__ issues
        if hasattr(val, '__str__') and not isinstance(val, (str, int, float, Decimal)):
            try:
                val = str(val)
            except Exception as e:
                logger.warning(f"String conversion failed for {type(val)}: {e}")
                return f"<CONVERSION_ERROR: {type(val).__name__}>"
        
        target_upper = target_type.upper()
        
        # String types
        if any(t in target_upper for t in ['VARCHAR', 'CHAR', 'CLOB', 'TEXT']):
            str_val = str(val) if not isinstance(val, str) else val
            if max_length and len(str_val) > max_length:
                return str_val[:max_length]
            return str_val
        
        # Integer types
        elif any(t in target_upper for t in ['INTEGER', 'BIGINT', 'SMALLINT', 'INT']):
            try:
                if isinstance(val, str) and not val.strip():
                    return None
                int_val = int(float(val))
                limits = {
                    'SMALLINT': (-32768, 32767),
                    'INTEGER': (-2147483648, 2147483647),
                    'BIGINT': (-9223372036854775808, 9223372036854775807)
                }
                for key, (lo, hi) in limits.items():
                    if key in target_upper:
                        return max(lo, min(hi, int_val))
                return int_val
            except (ValueError, TypeError):
                return None
        
        # Decimal/Numeric types
        elif any(t in target_upper for t in ['DECIMAL', 'NUMERIC', 'NUMBER']):
            try:
                if isinstance(val, str) and not val.strip():
                    return None
                return Decimal(str(val))
            except (ValueError, TypeError, InvalidOperation):
                return None
        
        # Float types
        elif any(t in target_upper for t in ['FLOAT', 'REAL', 'DOUBLE']):
            try:
                if isinstance(val, str) and not val.strip():
                    return None
                return float(val)
            except (ValueError, TypeError):
                return None
        
        # Date/Time types
        elif 'TIMESTAMP' in target_upper:
            str_val = str(val)
            if len(str_val) > 19:
                return str_val[:19]
            return str_val
        
        elif 'DATE' in target_upper:
            str_val = str(val)
            if len(str_val) > 10:
                return str_val[:10]
            return str_val
        
        elif 'TIME' in target_upper:
            str_val = str(val)
            if len(str_val) > 8:
                return str_val[:8]
            return str_val
        
        # Default fallback
        else:
            return str(val)
            
    except Exception as e:
        # Safe logging without trying to convert problematic objects
        logger.warning(f"Sanitize error for type {target_type}: {type(e).__name__}: {e}")
        return None

# ─────────────────────── ENHANCED FAST WORKER WITH BETTER ERROR HANDLING ───────────────────────
def fast_insert_worker(data_queue, schema, table, column_info, stats, stop_event):
    thread_name = threading.current_thread().name
    conn_raw = None
    dbi_conn = None
    cursor = None
    inserted = 0

    try:
        conn_raw = get_db2_connection_fast()
        dbi_conn = DBI_Connection(conn_raw)
        cursor = dbi_conn.cursor()

        column_names = [col['column_name'].upper() for col in column_info]
        col_types = {col['column_name'].upper(): col['data_type'] for col in column_info}
        col_lengths = {col['column_name'].upper(): col.get('length') for col in column_info}

        placeholders = ', '.join(['?' for _ in column_names])
        col_names_str = ', '.join([f'"{c}"' for c in column_names])
        insert_sql = f'INSERT INTO "{schema.upper()}"."{table.upper()}" ({col_names_str}) VALUES ({placeholders})'

        while not stop_event.is_set():
            try:
                batch = data_queue.get(timeout=5.0)
                if batch is None:
                    break

                sanitized_batch = []
                for row_idx, row in enumerate(batch):
                    sanitized_row = []
                    for col in column_names:
                        try:
                            raw_val = row.get(col)
                            sanitized_val = improved_sanitize_value(raw_val, col_types[col], col_lengths[col])
                            sanitized_row.append(sanitized_val)
                        except Exception as e:
                            logger.warning(f"{thread_name} sanitization of column {col} failed: {e}")
                            sanitized_row.append(None)
                    sanitized_batch.append(tuple(sanitized_row))

                if not sanitized_batch:
                    data_queue.task_done()
                    continue

                try:
                    cursor.executemany(insert_sql, sanitized_batch)
                    dbi_conn.commit()
                    inserted += len(sanitized_batch)
                    logger.debug(f"{thread_name} batch inserted {len(sanitized_batch)} rows into {schema}.{table}")
                except Exception as e:
                    dbi_conn.rollback()
                    logger.warning(f"{thread_name} batch insert failed: {e}, falling back to individual inserts")
                    for idx, row in enumerate(sanitized_batch):
                        try:
                            cursor.execute(insert_sql, row)
                            dbi_conn.commit()
                            inserted += 1
                        except Exception as ex:
                            dbi_conn.rollback()
                            logger.error(f"{thread_name} insert failed for row {idx} in {schema}.{table}: {ex}")
                            try:
                                log_failed_row(schema, table, idx, batch[idx], str(ex))
                            except:
                                logger.debug("Could not log failed row data")

                data_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"{thread_name} worker error: {e}")
                continue

    except Exception as e:
        logger.error(f"{thread_name} worker failed to initialize: {e}")

    finally:
        with stats['lock']:
            stats['total_inserted'] += inserted
        try:
            if cursor:
                cursor.close()
        except:
            pass
        try:
            if dbi_conn:
                dbi_conn.close()
        except:
            pass

# ─────────────────────── OPTIMIZED BATCH INSERT ───────────────────────
def optimized_batch_insert(schema, table, data_generator, batch_size=1000, num_workers=None):
    if num_workers is None:
        num_workers = min(5, multiprocessing.cpu_count())
    logger.info(f"🚀 INSERT: {schema}.{table} ({num_workers} workers)")
    column_info = get_table_column_info(schema, table)
    if not column_info:
        logger.error(f"No column info for {schema}.{table}")
        return 0
    stats = {"total_inserted": 0, "lock": threading.Lock()}
    data_queue = queue.Queue(maxsize=num_workers * 2)
    stop_event = threading.Event()
    workers = []
    for i in range(num_workers):
        t = threading.Thread(target=fast_insert_worker, args=(data_queue, schema, table, column_info, stats, stop_event), name=f"FastWorker-{i}")
        t.daemon = True
        t.start()
        workers.append(t)
    batch_buf = []
    try:
        for batch in data_generator:
            for row in batch:
                if isinstance(row, dict) and 'error' not in row:
                    batch_buf.append(row)
                if len(batch_buf) >= batch_size:
                    data_queue.put(batch_buf.copy(), timeout=30)
                    batch_buf.clear()
        if batch_buf:
            data_queue.put(batch_buf.copy(), timeout=30)
        data_queue.join()
    finally:
        stop_event.set()
        for _ in workers:
            data_queue.put(None)
        for t in workers:
            t.join()
    logger.info(f"✅ INSERTED: {stats['total_inserted']:,} rows")
    return stats['total_inserted']

# ─────────────────────── IMPROVED TABLE MIGRATION ───────────────────────
def improved_table_migration(
    source_type: str,
    source_schema: str,
    target_schema: str,
    table: str,
    table_info: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Improved table migration with better error handling"""
    
    start_time = time.time()
    result = {
        "table": table,
        "status": "failed",
        "rows_migrated": 0,
        "duration": 0,
        "error": None
    }
    
    try:
        # Import services
        if source_type.lower() == 'oracle':
            from services.oracle_service import (
                fetch_table_metadata as fetch_metadata,
                fetch_table_data_generator as fetch_data
            )
        elif source_type.lower() == 'sql':
            from services.sql_service import (
                fetch_table_metadata as fetch_metadata,
                fetch_table_data_generator as fetch_data
            )
        else:
            result["error"] = f"Unsupported source type: {source_type}"
            return result
        
        # Get metadata
        metadata = fetch_metadata(source_schema, table)
        if not metadata:
            result["error"] = "No metadata found"
            return result
        
        # Create table if needed
        if not check_table_exists(target_schema, table):
            ddl = generate_table_ddl_db2(target_schema, table, metadata, source_type)

            if not execute_db2_ddl(ddl):
                result["error"] = "Failed to create table"
                return result
        
        # Get data with reasonable batch size
        data_generator = fetch_data(source_schema, table, batch_size=1000)
        
        # Use optimized insert
        total_rows = optimized_batch_insert(target_schema, table, data_generator)
        
        result["status"] = "success"
        result["rows_migrated"] = total_rows
        result["duration"] = time.time() - start_time
        
        rate = total_rows / result["duration"] if result["duration"] > 0 else 0
        logger.info(f"🎉 MIGRATION SUCCESS: {table} - {total_rows:,} rows in {result['duration']:.2f}s ({rate:.0f} rows/sec)")
        
    except Exception as e:
        result["error"] = str(e)
        result["duration"] = time.time() - start_time
        logger.error(f"❌ MIGRATION FAILED: {table} - {e}")
    
    return result

# ─────────────────────── ORIGINAL FUNCTIONS (UPDATED) ───────────────────────
def get_table_row_count(schema: str, table: str) -> int:
    """Get row count for a table"""
    conn = get_db2_connection_fast()
    try:
        query = f'SELECT COUNT(*) as row_count FROM "{schema.upper()}"."{table.upper()}"'
        result = ibm_db.exec_immediate(conn, query)
        
        if result:
            row = ibm_db.fetch_assoc(result)
            return int(row['ROW_COUNT']) if row else 0
        return 0
        
    except Exception:
        return 0

def generate_table_ddl_db2(schema: str, table: str, metadata: List[Dict[str, Any]], source_type: str) -> str:
    col_defs = []
    for col in metadata:
        try:
            col_name = col.get("column_name")
            data_type = col.get("data_type")
            if not col_name or not data_type:
                continue
            if source_type.lower() == "oracle":
                length = col.get("data_length")
                precision = col.get("data_precision")
                scale = col.get("data_scale")
                nullable = col.get("nullable", "Y")
                db2_type = oracle_to_db2_type(data_type, length, precision, scale)
            elif source_type.lower() == "sql":
                length = col.get("character_maximum_length")
                precision = col.get("numeric_precision")
                scale = col.get("numeric_scale")
                nullable = col.get("is_nullable", "YES")
                db2_type = sql_to_db2_type(data_type, length, precision, scale)
            else:
                continue
            nullable_str = "NOT NULL" if nullable in ["N", "NO", "NOT NULL"] else ""
            col_defs.append(f'"{col_name.upper()}" {db2_type} {nullable_str}'.strip())
        except:
            continue
    return f'CREATE TABLE "{schema.upper()}"."{table.upper()}" (\n  ' + ",\n  ".join(col_defs) + "\n)"


def execute_db2_ddl(ddl: str, expect_result: bool = False):
    try:
        conn = get_db2_connection_fast()
        dbi_conn = DBI_Connection(conn)
        cursor = dbi_conn.cursor()
        cursor.execute(ddl)

        if expect_result:
            result = cursor.fetchall()
            return result

        dbi_conn.commit()
        return True
    except Exception as e:
        logger.error(f"[❌ DB2 EXEC ERROR]\nDDL: {ddl}\nERROR: {e}")
        return [] if expect_result else False
    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            dbi_conn.close()
        except:
            pass

def create_table_parallel_worker(table_info: Tuple[str, str, List[Dict[str, Any]], str], results: Dict[str, str]):
    schema, table, metadata, source_type = table_info
    try:
        if check_table_exists(schema, table):
            results[table] = "exists"
            return
        ddl = generate_table_ddl_db2(schema, table, metadata, source_type)
        success = execute_db2_ddl(ddl)
        results[table] = "created" if success else "failed"
    except Exception as e:
        results[table] = f"error: {str(e)}"


def create_tables_multithreaded(schema: str, table_metadata_list: List[Tuple[str, List[Dict[str, Any]], str]], max_workers: int = None) -> Dict[str, str]:
    """Parallel table creation"""
    max_workers = max_workers or min(6, multiprocessing.cpu_count())
    results = {}
    tasks = [(schema, tbl, meta, source) for tbl, meta, source in table_metadata_list]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(create_table_parallel_worker, task, results): task for task in tasks}
        for future in as_completed(futures):
            pass  # all result recording is done inside the worker
    return results
# ─────────────────────── UPDATED FUNCTION ALIASES ───────────────────────
def maximum_speed_batch_insert(schema: str, table: str, data_generator, batch_size: int = 1000, num_workers: int = None) -> int:
    """Updated to use optimized insert"""
    return optimized_batch_insert(schema, table, data_generator, batch_size, num_workers)

def ultra_fast_table_migration(source_type: str, source_schema: str, target_schema: str, table: str, table_info: Dict[str, Any] = None) -> Dict[str, Any]:
    """Updated to use improved migration"""
    return improved_table_migration(source_type, source_schema, target_schema, table, table_info)

def ultra_fast_batch_insert(schema: str, table: str, data_generator, batch_size: int = 1000, num_workers: int = None, queue_size: int = 50, commit_interval: int = 10000) -> int:
    """Updated to use optimized insert"""
    return optimized_batch_insert(schema, table, data_generator, batch_size, num_workers)

def migrate_table_parallel_optimized(source_type: str, source_schema: str, target_schema: str, table: str, table_info: Dict[str, Any] = None, batch_size: int = 1000, num_workers: int = None, queue_size: int = 50) -> Dict[str, Any]:
    """Updated to use improved migration"""
    return improved_table_migration(source_type, source_schema, target_schema, table, table_info)

def insert_table_data_db2(schema: str, table: str, rows: List[Dict[str, Any]]) -> int:
    """Legacy function - now uses optimized insert"""
    if not rows:
        return 0
    
    def row_generator():
        yield rows
    
    return optimized_batch_insert(schema, table, row_generator())

def bulk_insert_table_data_db2(schema: str, table: str, rows: List[Dict[str, Any]], chunk_size: int = 1000) -> int:
    """Legacy bulk insert - now uses optimized insert"""
    if not rows:
        return 0
    
    def row_generator():
        for i in range(0, len(rows), chunk_size):
            yield rows[i:i + chunk_size]
    
    return optimized_batch_insert(schema, table, row_generator())

def get_schema_migration_stats(source_type: str, source_schema: str) -> Dict[str, Any]:
    """Get migration statistics with reasonable estimates"""
    try:
        if source_type.lower() == 'oracle':
            from services.oracle_service import fetch_tables
        elif source_type.lower() == 'sql':
            from services.sql_service import fetch_tables
        else:
            return {"error": f"Unsupported source type: {source_type}"}
        
        tables = fetch_tables(source_schema)
        if not tables:
            return {"error": "No tables found"}
        
        # Use reasonable number of threads
        recommended_threads = min(6, multiprocessing.cpu_count())
        
        return {
            "total_tables": len(tables),
            "recommended_threads": recommended_threads,
            "available_cores": multiprocessing.cpu_count(),
            "estimated_duration": f"{len(tables) * 2:.1f} minutes",  # More realistic estimate
            "tables": tables
        }
        
    except Exception as e:
        return {"error": str(e)}

def get_source_row_count(source_type: str, source_schema: str, table: str) -> int:
    """Get source row count"""
    try:
        if source_type.lower() == 'oracle':
            from services.oracle_service import get_table_row_count as oracle_row_count
            return oracle_row_count(source_schema, table)
        elif source_type.lower() == 'sql':
            from services.sql_service import get_table_row_count as sql_row_count
            return sql_row_count(source_schema, table)
        else:
            return 0
    except Exception:
        return 0

def cleanup_connections():
    """Clean up all connections"""
    close_all_connections()
    logger.info("🧹 All connections cleaned up")

def close_thread_connection():
    """Close thread connection - compatibility"""
    pass

def check_schema_exists(schema: str) -> bool:
    """Check if schema exists"""
    conn = get_db2_connection_fast()
    try:
        stmt = f"SELECT SCHEMANAME FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = '{schema.upper()}'"
        result = ibm_db.exec_immediate(conn, stmt)
        return bool(result and ibm_db.fetch_assoc(result))
    except Exception:
        return False

def get_table_column_info(schema: str, table: str) -> List[Dict[str, Any]]:
    """Get table column info - uses cached version"""
    return get_table_column_info_cached(schema, table)

def log_failed_row(schema: str, table: str, row_index: int, data: Dict[str, Any], error: str):
    """Log failed row"""
    log_file = f"logs/failed_inserts_{schema}_{table}.log"
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    with open(log_file, "a", encoding="utf-8") as f:
        entry = {"index": row_index, "error": error, "data": data}
        f.write(json.dumps(entry, default=str) + "\n")
        
        
        
        
       # Add this to your db2_service.py after the fast_insert_worker function:

def monitor_migration_progress(schema, table, expected_rows, timeout_seconds=300):
    
    
    """Monitor migration progress and detect stalls"""
    start_time = time.time()
    last_count = 0
    stall_count = 0
    
    if expected_rows == 0:
        print(f"⚠️ Skipping monitor for {table}: expected_rows=0")
        return True
    
    while time.time() - start_time < timeout_seconds:
        try:
            current_count = get_table_row_count(schema, table)
            print(f"📊 {table}: {current_count:,}/{expected_rows:,} rows ({(current_count/expected_rows*100):.1f}%)")
            
            if current_count == last_count:
                stall_count += 1
                if stall_count >= 3:  
                    print(f"⚠️ STALL DETECTED: {table} stuck at {current_count:,} rows")
                    return False
            else:
                stall_count = 0
                last_count = current_count
            
            if current_count >= expected_rows:
                print(f"✅ COMPLETED: {table} - {current_count:,} rows")
                return True
                
        except Exception as e:
            print(f"❌ Monitor error: {e}")
            
        time.sleep(10)  # Check every 10 seconds
    
    print(f"⏰ TIMEOUT: {table} migration exceeded {timeout_seconds}s")
    return False

def improved_table_migration_with_monitoring(
    source_type: str,
    source_schema: str,
    target_schema: str,
    table: str,
    table_info: Dict[str, Any] = None,
    timeout_minutes: int = 15,      # per-table timeout
    retry_count: int = 0,
    max_retries: int = 1            # only retry once by default
) -> Dict[str, Any]:
    """Enhanced migration with progress monitoring and safe retry"""

    start_time = time.time()
    timeout_seconds = timeout_minutes * 60

    result = {
        "table": table,
        "status": "failed",
        "rows_migrated": 0,
        "duration": 0,
        "error": None
    }

    try:
        # Import services
        if source_type.lower() == 'oracle':
            from services.oracle_service import (
                fetch_table_metadata as fetch_metadata,
                fetch_table_data_generator as fetch_data,
                get_table_row_count as get_source_count
            )
        elif source_type.lower() == 'sql':
            from services.sql_service import (
                fetch_table_metadata as fetch_metadata,
                fetch_table_data_generator as fetch_data,
                get_table_row_count as get_source_count
            )
        else:
            result["error"] = f"Unsupported source type: {source_type}"
            return result

        expected_rows = get_source_count(source_schema, table)
        print(f"🎯 Target: {table} - {expected_rows:,} rows (expected)")
        
        metadata = fetch_metadata(source_schema, table)
        if not metadata:
            result["error"] = "No metadata found"
            return result

        # Ensure safe table creation only if it doesn't exist
        if check_table_exists(target_schema, table, skip_cache=True):
            logger.info(f"✅ Table already exists: {target_schema}.{table}, skipping DDL")
        else:
            ddl = generate_table_ddl_db2(target_schema, table, metadata, source_type)
            if not execute_db2_ddl(ddl):
                result["error"] = "Failed to create table"
                return result

        # Start async migration thread
        migration_thread = threading.Thread(
            target=lambda: _run_migration_worker(source_type, source_schema, target_schema, table, result),
            daemon=True
        )
        migration_thread.start()

        # Monitor progress
        success = monitor_migration_progress(target_schema, table, expected_rows, timeout_seconds)

        # Wait for thread to finish
        migration_thread.join(timeout=30)

        final_count = get_table_row_count(target_schema, table)

        if final_count == expected_rows:
            result["status"] = "success"
            result["rows_migrated"] = final_count
            result["error"] = None
            print(f"✅ Table {table} fully migrated: {final_count:,} rows")
        else:
            result["status"] = "failed"
            result["rows_migrated"] = final_count
            result["error"] = f"Incomplete insert: only {final_count:,}/{expected_rows:,} rows"
            print(f"❌ Incomplete insert: {final_count:,}/{expected_rows:,} rows")

        result["duration"] = time.time() - start_time

    except Exception as e:
        result["error"] = str(e)
        result["duration"] = time.time() - start_time
        logger.error(f"❌ MIGRATION FAILED: {table} - {e}")

    # Retry logic — prevent infinite loops
    if result["status"] == "failed" and "Incomplete" in result.get("error", ""):
        final_count = get_table_row_count(target_schema, table)
        
        # Prevent retry if we've already inserted more than expected
        if final_count >= expected_rows:
            logger.warning(f"⚠️ Skipping retry: {final_count} ≥ {expected_rows}")
            result["status"] = "success"
            result["rows_migrated"] = final_count
            result["error"] = None
            return result

        # Stop if retry limit reached
        if retry_count >= max_retries:
            logger.error(f"🛑 RETRIES EXCEEDED for {table} ({retry_count}/{max_retries})")
            return result
        
        # Retry after truncating table
        logger.warning(f"🔁 RETRYING {table} (retry {retry_count + 1}/{max_retries}) — Truncating and restarting...")
        truncate_table(target_schema, table)
        time.sleep(3)

        return improved_table_migration_with_monitoring(
            source_type=source_type,
            source_schema=source_schema,
            target_schema=target_schema,
            table=table,
            table_info=table_info,
            timeout_minutes=timeout_minutes,
            retry_count=retry_count + 1,
            max_retries=max_retries
        )

    return result


def _run_migration_worker(source_type, source_schema, target_schema, table, result: Dict[str, Any]):
    """Background migration worker"""
    try:
        if source_type.lower() == 'oracle':
            from services.oracle_service import fetch_table_data_generator as fetch_data
        else:
            from services.sql_service import fetch_table_data_generator as fetch_data

        data_generator = fetch_data(source_schema, table, batch_size=1000)
        total_rows = optimized_batch_insert(target_schema, table, data_generator, num_workers=10)

        # Store result directly
        result["_background_rows"] = total_rows
        result["_background_complete"] = True

    except Exception as e:
        result["_background_error"] = str(e)
        result["_background_complete"] = False





def truncate_table(schema: str, table: str):
    conn = get_db2_connection_fast()
    try:
        ibm_db.rollback(conn)  # Roll back any open transaction first
        stmt = f'TRUNCATE TABLE "{schema.upper()}"."{table.upper()}" IMMEDIATE'
        ibm_db.exec_immediate(conn, stmt)
        ibm_db.commit(conn)    # Commit the truncate
        logger.info(f"🧹 Truncated table {schema}.{table} before retry")
    except Exception as e:
        logger.error(f"❌ Failed to truncate table {schema}.{table}: {e}")
