import re
import ibm_db
from utils.credentials_store import load_credentials
from services.db2_service import check_table_exists


import re

import re

def convert_view_ddl_to_db2(source_ddl: str) -> str:
    """
    Convert Oracle/SQL Server CREATE VIEW DDL to DB2-compliant DDL with:
    - Explicit column list (DB2 requirement)
    - Unique aliases for duplicate columns.
    - Preserves entire SELECT statement intact except column aliases.
    """

    ddl = source_ddl.strip()
    ddl = re.sub(r"CREATE\s+(OR\s+REPLACE\s+)?VIEW", "CREATE OR REPLACE VIEW", ddl, flags=re.IGNORECASE)
    ddl = re.sub(r"\[([^\]]+)\]", r'"\1"', ddl)  # Convert [id] to "id"
    ddl = re.sub(r"\s+", " ", ddl).strip()

    pattern = re.compile(
        r'CREATE OR REPLACE VIEW\s+'
        r'(?:(?:"?(\w+)"?)\.)?'   # schema (group 1, optional)
        r'("?(\w+)"?)'            # view name (group 2 quoted, group 3 unquoted)
        r'\s+AS\s+'
        r'(SELECT\s+.+)',          # The full SELECT statement (group 4)
        re.IGNORECASE | re.DOTALL
    )

    m = pattern.match(ddl)
    if not m:
        raise Exception("Invalid VIEW DDL: Can't parse schema/view and SELECT statement.")

    schema = m.group(1)
    view_name = m.group(3)
    select_stmt = m.group(4).rstrip(";")

    schema_part = f'"{schema.upper()}"' if schema else None
    view_part = f'"{view_name.upper()}"'
    full_view_name = f"{schema_part}.{view_part}" if schema_part else view_part

    # --- Extract SELECT column list safely ---
    # Find the first top-level FROM keyword outside of parentheses

    def split_select_from(select_sql: str):
        """
        Split select_sql into (columns_part, rest_part),
        where columns_part is the text after SELECT and before first top-level FROM.
        rest_part is FROM ... and everything after.
        """

        select_sql = select_sql.strip()
        # Strip leading SELECT keyword
        if select_sql[:6].upper() == "SELECT":
            select_sql = select_sql[6:].lstrip()

        level = 0
        for i in range(len(select_sql)):
            c = select_sql[i]
            if c == '(':
                level += 1
            elif c == ')':
                if level > 0:
                    level -= 1
            elif c.upper() == 'F' and select_sql[i:i+4].upper() == "FROM" and level == 0:
                # Found top-level FROM
                return select_sql[:i].rstrip(), select_sql[i:].lstrip()

        raise Exception("Unable to find top-level FROM in SELECT statement.")

    try:
        columns_part, rest_sql = split_select_from(select_stmt)
    except Exception as ex:
        raise Exception(f"Failed to split SELECT and FROM: {ex}")

    # Split columns by commas not inside parentheses
    col_list = []
    level = 0
    start = 0
    for i, c in enumerate(columns_part):
        if c == '(':
            level += 1
        elif c == ')':
            level -= 1
        elif c == ',' and level == 0:
            col_list.append(columns_part[start:i].strip())
            start = i + 1
    col_list.append(columns_part[start:].strip())  # last column

    # Extract base column names to detect duplicates
    def extract_base_name(col: str) -> str:
        col = col.strip()

        # Detect alias after AS keyword, e.g. colname AS alias
        m_alias = re.search(r"\s+AS\s+\"?([\w\d_]+)\"?$", col, re.IGNORECASE)
        if m_alias:
            return m_alias.group(1).upper()

        # Detect alias without AS: e.g. colname alias
        parts = col.split()
        if len(parts) > 1:
            return parts[-1].strip('"').upper()

        # Remove table qualifiers
        if '.' in col:
            col = col.split('.')[-1]

        # Strip quotes
        return col.strip('"').upper()

    counts = {}
    for col in col_list:
        base_name = extract_base_name(col)
        counts[base_name] = counts.get(base_name, 0) + 1

    duplicate_counters = {}
    col_aliases = []
    final_col_names = []

    for col in col_list:
        base_name = extract_base_name(col)
        if counts[base_name] > 1:
            duplicate_counters[base_name] = duplicate_counters.get(base_name, 0) + 1
            alias_name = f"{base_name}_{duplicate_counters[base_name]}"
            col_aliases.append((col, alias_name))
            final_col_names.append(alias_name)
        else:
            col_aliases.append((col, None))
            final_col_names.append(base_name)

    # Build the aliased column list (add AS "alias" on duplicates)
    new_cols_text = []
    for original_col, alias in col_aliases:
        col_clean = original_col.rstrip(";")
        if alias:
            new_cols_text.append(f"{col_clean} AS \"{alias}\"")
        else:
            new_cols_text.append(col_clean)

    new_cols_text_joined = ", ".join(new_cols_text)

    # Rebuild the full SELECT statement with aliased columns
    new_select_stmt = f"SELECT {new_cols_text_joined} {rest_sql}"

    # Compose the final DB2-compliant DDL with explicit column list
    quoted_col_list = [f'"{col}"' for col in final_col_names]
    ddl_out = f"CREATE OR REPLACE VIEW {full_view_name} ({', '.join(quoted_col_list)}) AS {new_select_stmt};"

    # Debug log
    print(f"[DEBUG] Converted DB2 DDL:\n{ddl_out}")

    return ddl_out



def execute_view_ddl(ddl: str, schema: str) -> bool:
    """
    Execute the given DB2 DDL string for creating/replacing a view.
    Validates that all referenced tables exist before execution.
    """

    try:
        missing_tables = []
        required_tables = extract_table_names_from_ddl(ddl)
        for sch, tbl in required_tables:
            actual_schema = sch if sch else schema
            if not check_table_exists(actual_schema, tbl, skip_cache=True):
                missing_tables.append(f"{actual_schema}.{tbl}")

        if missing_tables:
            print(f"❌ View creation failed - missing referenced tables: {', '.join(missing_tables)}")
            return False

        creds = load_credentials("db2", is_target=True)
        dsn = (
            f"DATABASE={creds['database']};HOSTNAME={creds['host']};PORT={creds['port']};"
            f"UID={creds['username']};PWD={creds['password']};SECURITY={creds.get('security', 'SSL')};"
            f"CHARSET=UTF-8;AUTOCOMMIT=0;CONNECTTIMEOUT=30;QUERYTIMEOUT=300;CURRENTSCHEMA={schema};"
        )

        conn = ibm_db.connect(dsn, "", "")
        print(f"[DEBUG] Executing DDL:\n{ddl}")
        ibm_db.exec_immediate(conn, ddl)
        ibm_db.commit(conn)
        ibm_db.close(conn)

        return True
    except Exception as e:
        print(f"❌ View execution error: {e}")
        return False


def extract_table_names_from_ddl(ddl: str) -> list:
    """
    Extract list of referenced tables (with optional schema) from FROM and JOIN clauses
    in a SQL view DDL statement.

    Returns:
        List of tuples: (schema_or_None, table_name)
    """

    ddl_lower = ddl.lower()
    pattern = re.compile(r"from\s+([a-z0-9_\.]+)|join\s+([a-z0-9_\.]+)", re.IGNORECASE)
    matches = pattern.findall(ddl_lower)
    flattened = [item for pair in matches for item in pair if item]

    result = []
    for tbl_name in flattened:
        if '.' in tbl_name:
            sch, tbl = tbl_name.split('.', 1)
            result.append((sch.upper(), tbl.upper()))
        else:
            result.append((None, tbl_name.upper()))

    return result
