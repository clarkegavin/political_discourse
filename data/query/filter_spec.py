from typing import Dict, List, Any
from sqlalchemy.sql.elements import BinaryExpression


def build_filters(model, spec: Dict[str, Any]) -> List[BinaryExpression]:
    filters = []

    for field, value in spec.items():
        column = getattr(model, field)

        if isinstance(value, dict):
            if "from" in value:
                filters.append(column >= value["from"])
            if "to" in value:
                filters.append(column <= value["to"])
            if "in" in value:
                filters.append(column.in_(value["in"]))
        else:
            filters.append(column == value)

    return filters
