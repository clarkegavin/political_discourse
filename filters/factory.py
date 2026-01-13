#filters/factory.py

class FilterFactory:
    """
    Factory class for creating filter instances.
    """
    _filters = {}

    @classmethod
    def register_filter(cls, name: str, filter_cls):
        cls._filters[name] = filter_cls

    @classmethod
    def create_filter(cls, name: str, **kwargs):
        filter_cls = cls._filters.get(name)
        if not filter_cls:
            raise ValueError(f"Filter '{name}' is not registered.")
        return filter_cls(**kwargs)