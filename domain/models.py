from __future__ import annotations  
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .ports import ITypeMapper  

@dataclass
class Column:
    name: str
    data_type: str
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_nullable: bool = True
    default_value: Optional[str] = None
    
    def to_postgresql_type(self, type_mapper: 'ITypeMapper') -> str:
        return type_mapper.map_type(self)

@dataclass
class Index:
    name: str
    columns: List[Dict[str, Any]]
    is_unique: bool = False

@dataclass
class Table:
    name: str
    columns: List[Column] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    indexes: List[Index] = field(default_factory=list)

@dataclass
class MigrationResult:
    table_name: str
    rows_migrated: int
    success: bool
    error: Optional[str] = None
    duration_seconds: float = 0.0

class MigrationStatistics:
    def __init__(self):
        self.tables_processed = 0
        self.total_rows = 0
        self.failed_tables: List[str] = []
        self.results: List[MigrationResult] = []

    def add_result(self, result: MigrationResult) -> None:
        self.results.append(result)
        if result.success:
            self.tables_processed += 1
            self.total_rows += result.rows_migrated
        else:
            self.failed_tables.append(result.table_name)
