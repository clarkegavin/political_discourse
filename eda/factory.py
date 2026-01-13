# eda/factory.py
from eda.class_balance_eda import ClassBalanceEDA
from eda.wordcloud_eda import WordCloudEDA

class EDAFactory:
    _registery={}

    @classmethod
    def register_eda(cls, name, eda_class):
        cls._registery[name]=eda_class

    @classmethod
    def get_eda(cls, name, **kwargs):
        eda_class=cls._registery.get(name)
        if eda_class is None:
            raise KeyError(f"EDA '{name}' is not registered. Available: {list(cls._registery.keys())}")
        return eda_class(**kwargs)