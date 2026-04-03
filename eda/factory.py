# eda/factory.py
from eda.class_balance_eda import ClassBalanceEDA
from eda.wordcloud_eda import WordCloudEDA

class EDAFactory:
    _registry={}

    @classmethod
    def register_eda(cls, name, eda_class):
        if name in cls._registry:
            return
        cls._registry[name]=eda_class

    @classmethod
    def get_eda(cls, name, **kwargs):
        eda_class=cls._registry.get(name)
        if eda_class is None:
            raise KeyError(f"EDA '{name}' is not registered. Available: {list(cls._registry.keys())}")
        return eda_class(**kwargs)