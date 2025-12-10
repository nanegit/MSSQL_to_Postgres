from abc import ABC, abstractmethod
from typing import List, Tuple
from .models import Table, Index, Column  


class ISourceDatabase(ABC):
    """Port - Source database (MSSQL)"""
    
    @abstractmethod
    def connect(self) -> None:
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        pass
    
    @abstractmethod
    def get_tables(self) -> List[str]:
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> Table:
        pass
    
    @abstractmethod
    def read_data_batch(self, table_name: str, columns: List[str], 
                       offset: int, batch_size: int) -> List[Tuple]:
        pass
    
    @abstractmethod
    def count_rows(self, table_name: str) -> int:
        pass


class ITargetDatabase(ABC):
    """Port - Target database (PostgreSQL)"""
    
    @abstractmethod
    def connect(self) -> None:
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        pass
    
    @abstractmethod
    def create_table(self, table: Table) -> None:
        pass
    
    @abstractmethod
    def insert_batch(self, table_name: str, columns: List[str], 
                    rows: List[Tuple]) -> None:
        pass
    
    @abstractmethod
    def create_indexes(self, table_name: str, indexes: List[Index]) -> None:
        pass
    
    @abstractmethod
    def begin_transaction(self) -> None:
        pass
    
    @abstractmethod
    def commit_transaction(self) -> None:
        pass
    
    @abstractmethod
    def rollback_transaction(self) -> None:
        pass


class ITypeMapper(ABC):
    """Port - Type mapping service"""
    
    @abstractmethod
    def map_type(self, column: Column) -> str:
        pass


class ILogger(ABC):
    """Port - Logging service"""
    
    @abstractmethod
    def info(self, message: str) -> None:
        pass
    
    @abstractmethod
    def warning(self, message: str) -> None:
        pass
    
    @abstractmethod
    def error(self, message: str) -> None:
        pass
