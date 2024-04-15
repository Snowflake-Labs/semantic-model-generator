from typing import Any, List, Optional

from pydantic.dataclasses import dataclass


@dataclass
class FQNParts:
    database: str
    schema_name: str
    table: str

    def __post_init__(self: Any) -> None:
        """Uppercase table name"""
        self.table = self.table.upper()


@dataclass
class Column:
    id_: int
    column_name: str
    column_type: str
    values: Optional[List[str]] = None
    comment: Optional[str] = (
        None  # comment field's to save the column comment user specified on the column
    )

    def __post_init__(self: Any) -> None:
        """
        Update column_type to cleaned up version, eg. NUMBER(38,0) -> NUMBER
        """

        self.column_type = self.column_type.split("(")[0].strip().upper()


@dataclass
class Table:
    id_: int
    name: str
    columns: List[Column]
    comment: Optional[str] = (
        None  # comment field's to save the table comment user specified on the table
    )

    def __post_init__(self: Any) -> None:
        for col in self.columns:
            if col.column_name == "":
                raise ValueError("column name in table must be nonempty")
