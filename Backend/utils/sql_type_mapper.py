
# utils/sql_type_mapper.py
def sql_to_db2_type(data_type, length=None, precision=None, scale=None) -> str:
    data_type = data_type.upper()

    if data_type in ['CHAR', 'NCHAR']:
        return f"CHAR({min(int(length or 1), 254)})"
    
    elif data_type in ['VARCHAR', 'NVARCHAR', 'VARCHAR2', 'NVARCHAR2']:
        return f"VARCHAR({min(int(length or 255), 32762)})"
    
    elif data_type in ['TEXT', 'NTEXT']:
        return "CLOB(2G)"
    
    elif data_type in ['TINYTEXT', 'SMALLTEXT']:
        return "CLOB(255)"
    
    elif data_type in ['INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'MEDIUMINT']:
        return "INTEGER"
    
    elif data_type == 'BIT':
        return "SMALLINT"
    
    elif data_type in ['FLOAT', 'REAL', 'DOUBLE']:
        return "DOUBLE"
    
    elif data_type in ['NUMERIC', 'DECIMAL', 'DEC']:
        if precision is not None and scale is not None:
            return f"DECIMAL({min(int(precision), 31)}, {min(int(scale), 31)})"
        elif precision is not None:
            return f"DECIMAL({min(int(precision), 31)}, 0)"
        return "DECIMAL(31,0)"
    
    elif data_type == 'DATE':
        return "DATE"
    
    elif data_type == 'TIME':
        return "TIME"
    
    elif data_type in ['DATETIME', 'TIMESTAMP']:
        return "TIMESTAMP"
    
    elif data_type in ['BINARY', 'VARBINARY', 'IMAGE']:
        return "BLOB(32767)"
    
    elif data_type in ['XML']:
        return "XML"
    
    elif data_type in ['UNIQUEIDENTIFIER']:
        return "CHAR(36)"
    
    else:
        return "VARCHAR(1024)"  # Fallback default
