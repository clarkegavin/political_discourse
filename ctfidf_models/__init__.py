from .factory import CTFIDFModelFactory
from .class_tfidf import ClassTfidfModel

# register default ctfidf model
CTFIDFModelFactory.register_ctfidf_model("class_tfidf", ClassTfidfModel)

__all__ = [
    "CTFIDFModelFactory",
    "ClassTfidfModel",
]

