from domain.ports import ITypeMapper
from domain.models import Column


class MSSQLToPostgreSQLTypeMapper(ITypeMapper):
    """MSSQL to PostgreSQL type mapper"""
    
    TYPE_MAP = {
        'int': 'INTEGER', 'bigint': 'BIGINT', 'smallint': 'SMALLINT',
        'tinyint': 'SMALLINT', 'bit': 'BOOLEAN', 'decimal': 'NUMERIC',
        'numeric': 'NUMERIC', 'money': 'NUMERIC(19,4)', 'smallmoney': 'NUMERIC(10,4)',
        'float': 'DOUBLE PRECISION', 'real': 'REAL', 'datetime': 'TIMESTAMP',
        'datetime2': 'TIMESTAMP', 'smalldatetime': 'TIMESTAMP', 'date': 'DATE',
        'time': 'TIME', 'datetimeoffset': 'TIMESTAMP WITH TIME ZONE',
        'char': 'CHAR', 'varchar': 'VARCHAR', 'text': 'TEXT',
        'nchar': 'CHAR', 'nvarchar': 'VARCHAR', 'ntext': 'TEXT',
        'binary': 'BYTEA', 'varbinary': 'BYTEA', 'image': 'BYTEA',
        'uniqueidentifier': 'UUID', 'xml': 'XML'
    }
    
    def map_type(self, column: Column) -> str:
        base_type = column.data_type.lower()
        if base_type not in self.TYPE_MAP:
            return 'TEXT'
        
        pg_type = self.TYPE_MAP[base_type]
        
        if base_type in ('char', 'varchar', 'nchar', 'nvarchar') and column.max_length:
            if column.max_length == -1:
                return 'TEXT'
            return f"{pg_type}({column.max_length})"
        
        if base_type in ('decimal', 'numeric') and column.precision:
            if column.scale:
                return f"NUMERIC({column.precision},{column.scale})"
            return f"NUMERIC({column.precision})"
        
        return pg_type
