"""
encoders package initialization
Exposes the factory and commonly used encoders for easy imports:
from encoders import EncoderFactory, SklearnLabelEncoder
"""
from .factory import EncoderFactory
from .label_encoder import SklearnLabelEncoder
from .one_hot_encoder import OneHotEncoder
from .multi_hot_encoder import MultiHotEncoder
from .ordinal_encoder import OrdinalEncoder


# Register
EncoderFactory.register("sklearn_label", SklearnLabelEncoder)
EncoderFactory.register("label", SklearnLabelEncoder)
EncoderFactory.register("one_hot", OneHotEncoder)
EncoderFactory.register("onehot", OneHotEncoder)
EncoderFactory.register("multihot", MultiHotEncoder)
EncoderFactory.register("multi_hot", MultiHotEncoder)
EncoderFactory.register("ordinal", OrdinalEncoder)
EncoderFactory.register("ord", OrdinalEncoder)


__all__ = ["EncoderFactory",
           "SklearnLabelEncoder",
           "OneHotEncoder",
           "MultiHotEncoder",
           "OrdinalEncoder",
           ]
