from typing import Dict, Any
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy.sql import ColumnElement


def build_order_by(
    model: DeclarativeMeta,
    spec: Dict[str, Any] | None
) -> ColumnElement | None:
    """
    Translate order_by spec into SQLAlchemy order expression.

    Example spec:
      { "column": "dateInserted", "direction": "desc" }
    """

    if not spec:
        return None

    column_name = spec.get("column")
    direction = spec.get("direction", "asc").lower()

    if not hasattr(model, column_name):
        raise ValueError(f"Invalid order_by column '{column_name}' for {model.__name__}")

    column = getattr(model, column_name)

    if direction == "asc":
        return column.asc()
    elif direction == "desc":
        return column.desc()
    else:
        raise ValueError(f"Invalid order_by direction '{direction}'")
