
# utils/oracle_type_mapper.py
def oracle_to_db2_type(data_type, length=None, precision=None, scale=None) -> str:
    data_type = data_type.upper()

    if data_type in ['CHAR']:
        return f"CHAR({min(int(length or 1), 254)})"
    
    elif data_type in ['NCHAR']:
        return f"CHAR({min(int(length or 1), 254)})"
    
    elif data_type in ['VARCHAR2', 'NVARCHAR2']:
        return f"VARCHAR({min(int(length or 1), 32762)})"
    
    elif data_type == 'NUMBER':
        if precision is None and scale is None:
            return "DECIMAL(31,0)"
        elif scale is not None and precision is not None:
            return f"DECIMAL({min(int(precision), 31)}, {min(int(scale), 31)})"
        elif precision is not None:
            return f"DECIMAL({min(int(precision), 31)}, 0)"
        else:
            return "DECIMAL(31,0)"

    elif data_type == 'DATE':
        return "DATE"
    
    elif data_type.startswith('TIMESTAMP'):
        return "TIMESTAMP"

    elif data_type == 'CLOB':
        return "CLOB(2G)"
    
    elif data_type == 'NCLOB':
        return "DBCLOB(2G)"
    
    elif data_type == 'BLOB':
        return "BLOB(2G)"
    
    elif data_type in ['RAW', 'LONG RAW']:
        return "BLOB(32767)"
    
    elif data_type == 'LONG':
        return "CLOB(32760)"
    
    elif data_type == 'FLOAT':
        return "DOUBLE"
    
    elif data_type == 'BINARY_FLOAT':
        return "REAL"
    
    elif data_type == 'BINARY_DOUBLE':
        return "DOUBLE"
    
    else:
        return "VARCHAR(1024)"  # Default fallback
