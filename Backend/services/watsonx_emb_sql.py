
# services/watsonx_emb_sql.py 


import re
import logging
import sys
import os

from ibm_watsonx_ai.foundation_models import ModelInference
from ibm_watsonx_ai import Credentials
from dotenv import load_dotenv
load_dotenv()
# ====== Credentials======

WATSONX_URL = os.getenv("WATSONX_URL")
WATSONX_API_KEY = os.getenv("WATSONX_API_KEY")
WATSONX_PROJECT_ID = os.getenv("WATSONX_PROJECT_ID")
WATSONX_MODEL_ID = os.getenv("WATSONX_MODEL_ID")


_model_instance = None
conversation_history = []

# ====== Deterministic regex rules ======
ORACLE_TO_DB2_RULES = {
    r"\bNVL\s*\(": "COALESCE(",
    r"\bNVL2\s*\(": "__NVL2__(",
    r"\bSYSDATE\b": "CURRENT DATE",
    r"\bSYSTIMESTAMP\b": "CURRENT TIMESTAMP",
    # Keep ROWNUM handling pattern but we handle with a safer function below if needed
    r"\bTO_DATE\s*\(": "__TO_DATE__(",
    r"\bTO_CHAR\s*\(": "__TO_CHAR__(",
    r"\bFROM\s+DUAL\b": "FROM SYSIBM.SYSDUMMY1",
    r"\(\+\)": "__ORA_OUTER__",   # temporary placeholder handled by outer-join fixer
    r"\bTRUNC\s*\(": "__TO_DATE__(",
}

# Post-normalization
POST_NORMALIZE_RULES = {
    r"__NVL2__\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\)": r"(CASE WHEN \1 IS NOT NULL THEN \2 ELSE \3 END)",
    # If someone uses CURRENT DATE - col  -> ensure DATE(col)
    r"(CURRENT\s+DATE)\s*-\s*(?!DATE\()([a-zA-Z_][\w]*)": r"\1 - DATE(\2)",
    r"__ORA_OUTER__": "",   # placeholder removal (we'll handle outer joins separately)
    r"__TO_DATE__\s*\(": "TO_DATE(",
    r"__TO_CHAR__\s*\(": "TO_CHAR(",
}

ORACLE_TO_DB2_FORMAT_MAP = {
    "YYYY": "YYYY",
    "YY": "YY",
    "MM": "MM",
    "MON": "MON",
    "MONTH": "MONTH",
    "DD": "DD",
    "HH24": "HH24",
    "HH12": "HH12",
    "MI": "MI",
    "SS": "SS",
}


# ---------- Helpers ----------
def strip_code_fences(text: str) -> str:
    """Remove ``` fences if present."""
    text = re.sub(r"^\s*```[a-zA-Z0-9]*\s*", "", text)
    text = re.sub(r"\n?\s*```\s*$", "", text)
    return text.strip()


def apply_rules(sql: str, rules: dict) -> str:
    for pattern, repl in rules.items():
        sql = re.sub(pattern, repl, sql, flags=re.IGNORECASE)
    return sql


def _map_format_tokens(fmt: str) -> str:
    tokens = re.findall(r"[A-Za-z]+|[^A-Za-z]", fmt)
    return "".join(ORACLE_TO_DB2_FORMAT_MAP.get(t.upper(), t) for t in tokens)


def _to_date_repl(m: re.Match) -> str:
    """
    TO_DATE rules:
      - TO_DATE(str, fmt) -> TIMESTAMP_FORMAT / DATE(TIMESTAMP_FORMAT)
      - TO_DATE(expr) -> DATE(expr)
    """
    args = m.group(1)
    parts = [p.strip() for p in re.split(r",(?![^']*')", args)]
    if len(parts) == 1:
        return f"DATE({parts[0]})"
    fmt = parts[1].strip().strip("'").strip('"')
    mapped = _map_format_tokens(fmt)
    if any(tok in mapped for tok in ("HH", "HH12", "HH24", "MI", "SS")):
        return f"TIMESTAMP_FORMAT({parts[0]}, '{mapped}')"
    return f"DATE(TIMESTAMP_FORMAT({parts[0]}, '{mapped}'))"


def _to_char_repl(m: re.Match) -> str:
    """
    TO_CHAR -> VARCHAR_FORMAT
    """
    args = m.group(1)
    parts = [p.strip() for p in re.split(r",(?![^']*')", args)]
    if len(parts) >= 2:
        fmt = parts[1].strip().strip("'").strip('"')
        mapped = _map_format_tokens(fmt)
        return f"VARCHAR_FORMAT({parts[0]}, '{mapped}')"
    return f"VARCHAR_FORMAT({args})"


def normalize_to_date_and_to_char(sql: str) -> str:
    sql = re.sub(r"\bTO_DATE\s*\(\s*(.*?)\s*\)", _to_date_repl, sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bTO_CHAR\s*\(\s*(.*?)\s*\)", _to_char_repl, sql, flags=re.IGNORECASE)
    return sql


# ----- Sequence rules (preserve sequence name) -----
def apply_sequence_rules(sql: str) -> str:
    # schema.seq.NEXTVAL  -> NEXT VALUE FOR schema.seq
    sql = re.sub(r"([A-Za-z_][\w\.]*)\.NEXTVAL\b",
                 lambda m: f"NEXT VALUE FOR {m.group(1)}",
                 sql, flags=re.IGNORECASE)
    sql = re.sub(r"([A-Za-z_][\w\.]*)\.CURRVAL\b",
                 lambda m: f"PREVIOUS VALUE FOR {m.group(1)}",
                 sql, flags=re.IGNORECASE)
    return sql


# ----- Outer join fixer for Oracle (+) ----- #
def fix_oracle_outer_join(sql: str) -> str:
    """
    Convert simple Oracle (+) outer join patterns into LEFT JOIN form.
    This implementation focuses on the common two-table pattern:
      FROM t1 a, t2 b WHERE a.col = b.col(+)
    -> FROM t1 a LEFT JOIN t2 b ON a.col = b.col

    For more complex multi-table cases we preserve original but try to
    convert any isolated equality with (+) that references two table aliases.
    """
    original_sql = sql

    # locate FROM ... (end at WHERE / GROUP / ORDER / UNION / FETCH / LIMIT / ;)
    from_search = re.search(r"\bFROM\b", sql, flags=re.IGNORECASE)
    if not from_search:
        return sql

    start_idx = from_search.end()
    # find end of from clause
    next_clause = re.search(r"\b(WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|UNION|FETCH|LIMIT)\b", sql[start_idx:], flags=re.IGNORECASE)
    if next_clause:
        from_part = sql[start_idx:start_idx + next_clause.start()]
        rest = sql[start_idx + next_clause.start():]
    else:
        # no further clause -> from_part to end (may include WHERE later but assume none)
        from_part = sql[start_idx:]
        rest = ""

    # split table list (simple split on commas outside parentheses)
    tables = [t.strip() for t in re.split(r',\s*(?![^()]*\))', from_part) if t.strip()]

    # Build alias->table mapping (preserve table text)
    alias_map = {}   # alias -> table_text
    alias_order = []  # preserve order
    for entry in tables:
        # handle "table AS alias" or "schema.table alias"
        tokens = re.split(r'\s+', entry)
        if len(tokens) == 1:
            tbl = tokens[0]
            alias = tokens[0]
        else:
            if len(tokens) >= 3 and tokens[-2].upper() == "AS":
                alias = tokens[-1]
                tbl = " ".join(tokens[:-2])
            else:
                alias = tokens[-1]
                tbl = " ".join(tokens[:-1])
        alias_map[alias] = tbl
        alias_order.append((alias, tbl))

    # find all equality conditions with (+) in the whole SQL (works if they appear in WHERE)
    plus_matches = list(re.finditer(r"([A-Za-z_][\w]*)\s*\.\s*([A-Za-z_][\w]*)\s*\(\+\)", sql))
    if not plus_matches:
        return sql

    # If there are exactly 2 tables and at least one plus-match referencing those aliases,
    # we can convert to LEFT JOIN(s) for those alias pairs.
    if len(alias_order) >= 2:
        # For simplicity, only handle common case where FROM lists two tables OR first conversion references two aliases
        # We'll iterate matches and attempt to convert the simplest patterns.
        converted_from = None
        where_remainder = sql
        # We'll work per-match and rebuild FROM if possible
        for m in plus_matches:
            plus_alias = m.group(1)  # alias that carries the (+)
            plus_col = m.group(2)
            # attempt to find the counterpart alias/column on the other side of equality
            # pattern like: <other_alias>.<other_col> = <plus_alias>.<plus_col>(+)
            # search around the match for an equality using regex
            # match full equality: otherAlias.otherCol\s*=\s*plusAlias.plusCol(+)
            # we try both directions
            eq_pattern_right = re.compile(r"([A-Za-z_][\w]*)\s*\.\s*([A-Za-z_][\w]*)\s*=\s*%s\s*\.\s*%s\(\+\)" % (re.escape(plus_alias), re.escape(plus_col)), flags=re.IGNORECASE)
            eq_pattern_left = re.compile(r"%s\s*\.\s*%s\(\+\)\s*=\s*([A-Za-z_][\w]*)\s*\.\s*([A-Za-z_][\w]*)" % (re.escape(plus_alias), re.escape(plus_col)), flags=re.IGNORECASE)
            eq_match = eq_pattern_right.search(sql) or eq_pattern_left.search(sql)
            if not eq_match:
                # try to capture equality where plus is on right hand side as 'a.col = b.col(+)'
                eq_match2 = re.search(r"([A-Za-z_][\w]*)\s*\.\s*([A-Za-z_][\w]*)\s*=\s*([A-Za-z_][\w]*)\s*\.\s*([A-Za-z_][\w]*)\(\+\)", sql)
                if eq_match2:
                    left_alias, left_col, right_alias, right_col = eq_match2.groups()
                    left_has_plus = False
                else:
                    continue
            else:
                # if eq_pattern_right matched, groups are otherAlias, otherCol
                # eq_match.group(1), eq_match.group(2) represent other alias and column
                if eq_pattern_right.search(sql):
                    other_alias, other_col = eq_match.group(1), eq_match.group(2)
                    # plus_alias is right side; so left = other_alias
                    left_alias, left_col = other_alias, other_col
                    right_alias, right_col = plus_alias, plus_col
                else:
                    # eq_pattern_left matched
                    other_alias, other_col = eq_match.group(1), eq_match.group(2)
                    left_alias, left_col = other_alias, other_col
                    right_alias, right_col = plus_alias, plus_col

            # Only convert when both aliases are present in the FROM table list
            if left_alias in alias_map and right_alias in alias_map:
                # Build an orderly FROM using left_alias as base and left JOIN right_alias (RIGHT alias had (+))
                left_tbl = alias_map[left_alias]
                right_tbl = alias_map[right_alias]

                # Build new FROM clause: start with left table, then LEFT JOIN right table on condition
                new_from_items = []
                used_aliases = set()
                new_from = f"FROM {left_tbl} {left_alias} LEFT JOIN {right_tbl} {right_alias} ON {left_alias}.{left_col} = {right_alias}.{right_col}"
                used_aliases.add(left_alias)
                used_aliases.add(right_alias)

                # Append remaining tables as simple JOINs (we will keep them comma-separated as fallback)
                remaining = []
                for a, t in alias_order:
                    if a not in used_aliases:
                        remaining.append(f"{t} {a}")

                if remaining:
                    # append the remaining tables using commas (to avoid messing other predicates)
                    new_from = new_from + ", " + ", ".join(remaining)

                # Remove the equality condition that contained (+) from the WHERE clause
                # Find the equality text (both variants) and remove it
                # Build escape for the pattern like: leftAlias.leftCol = rightAlias.rightCol(+)
                cond_variants = [
                    rf"{re.escape(left_alias)}\s*\.\s*{re.escape(left_col)}\s*=\s*{re.escape(right_alias)}\s*\.\s*{re.escape(right_col)}\s*\(\+\)",
                    rf"{re.escape(right_alias)}\s*\.\s*{re.escape(right_col)}\s*\(\+\)\s*=\s*{re.escape(left_alias)}\s*\.\s*{re.escape(left_col)}"
                ]
                new_where = sql
                for cv in cond_variants:
                    # remove with optional leading/trailing AND
                    new_where = re.sub(rf"(\s*\bAND\b\s*)?{cv}(\s*\bAND\b\s*)?", " ", new_where, flags=re.IGNORECASE)
                # replace old FROM part with new_from
                # locate original FROM span and replace
                # build final SQL
                pre_from = sql[:from_search.start()]
                # attempt to find end of FROM part in original to slice rest
                next_clause2 = re.search(r"\b(WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|UNION|FETCH|LIMIT)\b", sql[from_search.end():], flags=re.IGNORECASE)
                if next_clause2:
                    rest_after_from = sql[from_search.end() + next_clause2.start():]
                else:
                    rest_after_from = ""
                # Compose final SQL as: pre_from + new_from + (rest_after_from with the removed condition)
                # Clean up extra whitespace
                final_sql = (pre_from + new_from + " " + rest_after_from).strip()
                # Insert WHERE part from new_where (if it still contains other conditions)
                # Extract where clause from new_where if present
                where_match = re.search(r"\bWHERE\b(.*)", new_where, flags=re.IGNORECASE | re.DOTALL)
                if where_match:
                    remaining_where = where_match.group(1).strip()
                    # remove leftover excessive AND/OR at ends
                    remaining_where = re.sub(r"^\s*(AND|OR)\s+", "", remaining_where, flags=re.IGNORECASE)
                    remaining_where = re.sub(r"\s+(AND|OR)\s*$", "", remaining_where, flags=re.IGNORECASE)
                    if remaining_where:
                        final_sql = pre_from + new_from + " WHERE " + remaining_where
                    else:
                        final_sql = pre_from + new_from
                else:
                    # nothing in WHERE
                    final_sql = pre_from + new_from

                # preserve trailing clauses like GROUP BY, ORDER BY etc from original (attempt simple capture)
                trailing = ""
                trailing_match = re.search(r"\b(GROUP\s+BY|HAVING|ORDER\s+BY|UNION|FETCH|LIMIT)\b(.*)$", sql, flags=re.IGNORECASE | re.DOTALL)
                if trailing_match:
                    trailing = trailing_match.group(0)
                    # avoid duplicating WHERE content already handled
                    # append trailing if not already in final_sql
                    if trailing.strip() not in final_sql:
                        final_sql = final_sql + " " + trailing

                # cleanup: normalize spaces
                final_sql = re.sub(r"\s+", " ", final_sql).strip()
                return final_sql

    # fallback: no safe transformation found
    return original_sql


def apply_fallback_rules(sql: str) -> str:
    # Apply sequence conversion first (so NEXTVAL -> proper form)
    sql = apply_sequence_rules(sql)
    # Apply basic deterministic conversions
    sql = apply_rules(sql, ORACLE_TO_DB2_RULES)
    # Attempt to fix simple Oracle (+) outer joins
    sql = fix_oracle_outer_join(sql)
    # Apply post-normalization rules
    sql = apply_rules(sql, POST_NORMALIZE_RULES)
    # Normalize TO_DATE/TO_CHAR to Db2 variants
    sql = normalize_to_date_and_to_char(sql)
    # trim and collapse spaces
    sql = re.sub(r"[ \t]+", " ", sql)
    sql = re.sub(r"\s+\n", "\n", sql)
    return sql.strip()


# ---------- Fidelity-preserving fixes ----------
def high_fidelity_postprocess(original: str, converted: str) -> str:
    # Keep RANK() when original had RANK (avoid DENSE_RANK auto-swaps from LLM)
    if re.search(r"\bRANK\s*\(", original, flags=re.IGNORECASE) and not re.search(r"\bDENSE_RANK\s*\(", original, flags=re.IGNORECASE):
        converted = re.sub(r"\bDENSE_RANK\s*\(", "RANK(", converted, flags=re.IGNORECASE)
    return converted


# ---------- Watsonx Model setup ----------
def getModel():
    global _model_instance
    if _model_instance is not None:
        return _model_instance
    print("🔌: Connecting to Watsonx model...")
    try:
        creds = Credentials(url=WATSONX_URL, api_key=WATSONX_API_KEY)
        _model_instance = ModelInference(
            model_id=WATSONX_MODEL_ID,
            credentials=creds,
            project_id=WATSONX_PROJECT_ID,
        )
    except Exception as e:
        logging.error(f"Error initializing Watsonx model: {e}")
        _model_instance = None
    return _model_instance


# ---------- Final conversion (deterministic-first) ----------
def call_model(full_query: str) -> str:
    """
    Convert Oracle SQL -> Db2 SQL.
    Uses deterministic rules first; if advanced Oracle constructs remain,
    escalates to Watsonx model to finish conversion.
    """
    partial_sql = apply_fallback_rules(full_query)

    # If advanced Oracle-only constructs remain, escalate to LLM
    if re.search(r"\bCONNECT\s+BY\b|\bSTART\s+WITH\b|\bMATCH_RECOGNIZE\b|\bMODEL\b", partial_sql, flags=re.IGNORECASE):
        model = getModel()
        if not model:
            return ":warning: Model not available."
        prompt = (
            "You are an SQL migration expert for Oracle→Db2.\n"
            "The SQL provided is already partially converted to Db2 syntax.\n"
            "Convert ONLY remaining unsupported Oracle constructs (CONNECT BY, MATCH_RECOGNIZE, MODEL, etc.).\n"
            "Do NOT modify already converted functions.\n"
            "Return only the Db2 SQL query."
        )
        try:
            result = model.chat([
                {"role": "system", "content": prompt},
                {"role": "user", "content": partial_sql}
            ])
            # Extract content robustly for different result shapes
            try:
                content = result["choices"][0]["message"]["content"]
            except Exception:
                content = result.get("results") or result.get("predictions") or ""
            converted = strip_code_fences(content or "")
        except Exception as e:
            return f":x: Error calling model: {str(e)}"
    else:
        converted = partial_sql

    converted = high_fidelity_postprocess(full_query, converted)
    return converted.strip()


# ---------- CLI ----------
def main():
    logging.basicConfig(level=logging.ERROR, stream=sys.stderr)
    print(":speech_balloon: Oracle → Db2 Query Converter (Deterministic + Watsonx fallback)")
    print("Tip: For best Oracle compatibility in Db2: db2set db2_compatibility_vector=ORA ; restart instance.\n")
    print("Enter Oracle SQL (multi-line). Type 'convert' to run or 'quit' to exit.\n")

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n:wave: Goodbye!")
            break

        if user_input.lower() in {"quit", "exit"}:
            print(":wave: Goodbye!")
            break
        elif user_input.lower() == "convert":
            full_query = "\n".join(conversation_history).strip()
            if not full_query:
                print(":warning: No query entered.")
                continue
            db2_query = call_model(full_query)
            print("\n=== Converted Db2 SQL ===")
            print(db2_query)
            print("=========================\n")
            conversation_history.clear()
        else:
            conversation_history.append(user_input)





if __name__ == "__main__":
    main()
