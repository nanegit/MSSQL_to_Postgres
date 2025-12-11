from typing import Dict
from application.use_cases.migrate_db import MigrateDatabaseUseCase
from infrastructure.adapters.mssql_adapter import MSSQLAdapter
from infrastructure.adapters.postsql_adapter import PostgreSQLAdapter
from infrastructure.adapters.type_mapper import MSSQLToPostgreSQLTypeMapper
from infrastructure.adapters.logger_adapter import PythonLoggingAdapter


class MigrationServiceFactory:
    
    @staticmethod
    def create(mssql_config: Dict[str, str], pg_config: Dict[str, str],
              batch_size: int = 10000) -> MigrateDatabaseUseCase:
        type_mapper = MSSQLToPostgreSQLTypeMapper()
        logger = PythonLoggingAdapter()
        source_db = MSSQLAdapter(mssql_config, type_mapper)
        target_db = PostgreSQLAdapter(pg_config, type_mapper)
        
        return MigrateDatabaseUseCase(
            source_db=source_db,
            target_db=target_db,
            type_mapper=type_mapper,
            logger=logger,
            batch_size=batch_size
        )