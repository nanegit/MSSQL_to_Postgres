from datetime import datetime
from domain.ports import ISourceDatabase, ITargetDatabase, ITypeMapper, ILogger
from domain.models import MigrationStatistics, MigrationResult, Table



class MigrateDatabaseUseCase:
    
    def __init__(self, 
                 source_db: ISourceDatabase,
                 target_db: ITargetDatabase,
                 type_mapper: ITypeMapper,
                 logger: ILogger,
                 batch_size: int = 10000):
        self.source_db = source_db
        self.target_db = target_db
        self.type_mapper = type_mapper
        self.logger = logger
        self.batch_size = batch_size
        self.stats = MigrationStatistics()
    
    def execute(self) -> MigrationStatistics:
        try:
            self.logger.info("Կապեր հաստատվում են...")
            self.source_db.connect()
            self.target_db.connect()
            
            tables = self.source_db.get_tables()
            self.logger.info(f"Գտնված {len(tables)} աղյուսակ")
            
            for i, table_name in enumerate(tables, 1):
                self.logger.info(f"\n{'='*60}")
                self.logger.info(f"[{i}/{len(tables)}] Միգրացիա: {table_name}")
                self.logger.info(f"{'='*60}")
                
                result = self._migrate_table(table_name)
                self.stats.add_result(result)
                
                if result.success:
                    self.logger.info(
                        f"✓ Հաջող: {result.rows_migrated} տող, "
                        f"{result.duration_seconds:.2f}վ"
                    )
                else:
                    self.logger.error(f"✗ Ձախողում: {result.error}")
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info("ՄԻԳՐԱՑԻԱՆ ԱՎԱՐՏՎԱԾ")
            self.logger.info(f"{'='*60}")
            self.logger.info(f"Աղյուսակներ: {self.stats.tables_processed}")
            self.logger.info(f"Ընդամենը տողեր: {self.stats.total_rows}")
            self.logger.info(f"Ձախողված: {len(self.stats.failed_tables)}")
            
            return self.stats
            
        except Exception as e:
            self.logger.error(f"Ընդհանուր սխալ: {e}")
            raise
        finally:
            self.source_db.disconnect()
            self.target_db.disconnect()
    
    def _migrate_table(self, table_name: str) -> MigrationResult:
        start_time = datetime.now()
        
        try:
            self.logger.info("1. Սխեմայի ընթերցում...")
            table = self.source_db.get_table_schema(table_name)
            
            self.logger.info("2. Աղյուսակի ստեղծում...")
            self.target_db.create_table(table)
            
            self.logger.info("3. Տվյալների միգրացիա...")
            rows_migrated = self._migrate_table_data(table)
            
            self.logger.info("4. Ինդեքսների ստեղծում...")
            self.target_db.create_indexes(table_name, table.indexes)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            return MigrationResult(
                table_name=table_name,
                rows_migrated=rows_migrated,
                success=True,
                duration_seconds=duration
            )
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            return MigrationResult(
                table_name=table_name,
                rows_migrated=0,
                success=False,
                error=str(e),
                duration_seconds=duration
            )
    
    def _migrate_table_data(self, table: Table) -> int:
        total_rows = self.source_db.count_rows(table.name)
        
        if total_rows == 0:
            return 0
        
        self.logger.info(f"   Ընդամենը տողեր: {total_rows:,}")
        
        columns = [col.name for col in table.columns]
        offset = 0
        rows_migrated = 0
        
        while offset < total_rows:
            rows = self.source_db.read_data_batch(
                table.name, columns, offset, self.batch_size
            )
            
            if not rows:
                break
            
            self.target_db.begin_transaction()
            try:
                self.target_db.insert_batch(table.name, columns, rows)
                self.target_db.commit_transaction()
                
                rows_migrated += len(rows)
                offset += len(rows)
                
                progress = (offset / total_rows) * 100
                self.logger.info(f"   Progress: {offset:,}/{total_rows:,} ({progress:.1f}%)")
                
            except Exception as e:
                self.target_db.rollback_transaction()
                raise
        
        return rows_migrated
