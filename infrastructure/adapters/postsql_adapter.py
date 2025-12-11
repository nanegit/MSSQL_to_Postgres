
import psycopg2
from psycopg2.extras import execute_batch
from typing import Dict, List, Tuple
from domain.ports import ITargetDatabase, ITypeMapper
from domain.models import Table, Index


class PostgreSQLAdapter(ITargetDatabase):

    def __init__(self, config: Dict[str, str], type_mapper: ITypeMapper):
        self.config = config
        self.type_mapper = type_mapper
        self.connection = None

    def connect(self) -> None:
        temp_conn = psycopg2.connect(
            host=self.config['host'],
            port=self.config.get('port', 5432),
            user=self.config['username'],
            password=self.config['password']
        )
        temp_conn.autocommit = True
        temp_cursor = temp_conn.cursor()

        db_name = self.config['database']
        temp_cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'")
        exists = temp_cursor.fetchone()
        if not exists:
            temp_cursor.execute(f'CREATE DATABASE "{db_name}"')
            print(f"✓ PostgreSQL: Created database '{db_name}'")

        temp_cursor.close()
        temp_conn.close()

        self.connection = psycopg2.connect(
            host=self.config['host'],
            port=self.config.get('port', 5432),
            database=db_name,
            user=self.config['username'],
            password=self.config['password']
        )
        self.connection.autocommit = False

    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()

    def create_table(self, table: Table) -> None:
        cols_def = []
        for col in table.columns:
            pg_type = col.to_postgresql_type(self.type_mapper)
            nullable = "NULL" if col.is_nullable else "NOT NULL"
            cols_def.append(f'"{col.name}" {pg_type} {nullable}')

        if table.primary_keys:
            pk = ", ".join([f'"{c}"' for c in table.primary_keys])
            cols_def.append(f'PRIMARY KEY ({pk})')

        sql = f'CREATE TABLE IF NOT EXISTS "{table.name}" (\n  {",\n  ".join(cols_def)}\n)'

        cursor = self.connection.cursor()
        cursor.execute(sql)
        self.connection.commit()
        cursor.close()

    def insert_batch(self, table_name: str, columns: List[str], rows: List[Tuple]) -> None:
        cursor = self.connection.cursor()
        cols = ", ".join([f'"{c}"' for c in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f'INSERT INTO "{table_name}" ({cols}) VALUES ({placeholders})'
        execute_batch(cursor, sql, rows, page_size=len(rows))
        cursor.close()

    def create_indexes(self, table_name: str, indexes: List[Index]) -> None:
        cursor = self.connection.cursor()
        for idx in indexes:
            try:
                unique = "UNIQUE" if idx.is_unique else ""
                cols = ", ".join([f'"{c["name"]}"' + (" DESC" if c["desc"] else "") for c in idx.columns])
                idx_name = f'idx_{table_name}_{idx.name}'[:63]
                sql = f'CREATE {unique} INDEX IF NOT EXISTS "{idx_name}" ON "{table_name}" ({cols})'
                cursor.execute(sql)
                self.connection.commit()
            except:
                self.connection.rollback()
        cursor.close()

    def begin_transaction(self) -> None:
        pass

    def commit_transaction(self) -> None:
        self.connection.commit()

    def rollback_transaction(self) -> None:
        self.connection.rollback()
