import pyodbc
from typing import Dict, List, Tuple
from domain.ports import ISourceDatabase, ITypeMapper
from domain.models import Table, Column, Index


class MSSQLAdapter(ISourceDatabase):
    
    def __init__(self, config: Dict[str, str], type_mapper: ITypeMapper):
        self.config = config
        self.type_mapper = type_mapper
        self.connection = None
    
    def connect(self) -> None:
        conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={self.config['server']};"
        f"DATABASE={self.config['database']};"
        f"Trusted_Connection={self.config.get('trusted_connection', 'yes')}"
    )
        self.connection = pyodbc.connect(conn_str)

    
    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()
    
    def get_tables(self) -> List[str]:
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
    
    def get_table_schema(self, table_name: str) -> Table:
        table = Table(name=table_name)
        table.columns = self._get_columns(table_name)
        table.primary_keys = self._get_primary_keys(table_name)
        table.indexes = self._get_indexes(table_name)
        return table
    
    def _get_columns(self, table_name: str) -> List[Column]:
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
                   NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, (table_name,))
        
        columns = []
        for row in cursor.fetchall():
            columns.append(Column(
                name=row[0], data_type=row[1], max_length=row[2],
                precision=row[3], scale=row[4], is_nullable=(row[5] == 'YES')
            ))
        cursor.close()
        return columns
    
    def _get_primary_keys(self, table_name: str) -> List[str]:
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
            AND TABLE_NAME = ?
        """, (table_name,))
        pks = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return pks
    
    def _get_indexes(self, table_name: str) -> List[Index]:
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT i.name, i.is_unique, COL_NAME(ic.object_id, ic.column_id), ic.is_descending_key
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key = 0 AND i.type > 0
            ORDER BY i.name, ic.key_ordinal
        """, (table_name,))
        
        idx_dict = {}
        for row in cursor.fetchall():
            if row[0] not in idx_dict:
                idx_dict[row[0]] = Index(name=row[0], is_unique=row[1], columns=[])
            idx_dict[row[0]].columns.append({'name': row[2], 'desc': row[3]})
        
        cursor.close()
        return list(idx_dict.values())
    
    def count_rows(self, table_name: str) -> int:
        cursor = self.connection.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM [{table_name}]')
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    
    def read_data_batch(self, table_name: str, columns: List[str],
                       offset: int, batch_size: int) -> List[Tuple]:
        cursor = self.connection.cursor()
        cols = ", ".join([f'[{c}]' for c in columns])
        sql = f"SELECT {cols} FROM [{table_name}] ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        return rows

