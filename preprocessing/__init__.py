# preprocessing/__init__.py
# Expose preprocessing interfaces and default implementations
from .base import Preprocessor
from .factory import PreprocessorFactory
from .stemmer import Stemmer
from .lemmatizer import Lemmatizer
from .lowercase import Lowercase
from .stopword_remover import StopwordRemover
from .emoji_remover import EmojiRemover
from .filter_rows import FilterRows
from .mask_genre_words import MaskGenreWords
from .remove_duplicates import RemoveDuplicates
from .remove_urls import RemoveURLs
from .remove_repeated_characters import RemoveRepeatedCharacters
from .remove_punctuation_noise import RemovePunctuationNoise
from .remove_whitespace import RemoveWhitespace
from .merge_features import MergeFeatures
from .explode_columns import ExplodeColumns
from .remove_html_tags import RemoveHTMLTags
from .log_transform import LogTransform
from .temporal_features import TemporalFeatures
from .count_features import CountFeatures
from .catalog_count import CatalogCount
from .normalise_feature import NormaliseFeature
from .cyclic_encode import CyclicEncode
import nltk

# Register built-in preprocessors
PreprocessorFactory.register("stem", Stemmer)
PreprocessorFactory.register("stemmer", Stemmer)
PreprocessorFactory.register("lemmatize", Lemmatizer)
PreprocessorFactory.register("lemmatizer", Lemmatizer)
PreprocessorFactory.register("lowercase", Lowercase)
PreprocessorFactory.register("lower", Lowercase)
PreprocessorFactory.register("stopword_remover", StopwordRemover)
PreprocessorFactory.register("stopwords", StopwordRemover)
PreprocessorFactory.register("emoji_remover", EmojiRemover)
PreprocessorFactory.register("emoji", EmojiRemover)
PreprocessorFactory.register("filter_rows", FilterRows)
PreprocessorFactory.register("filter", FilterRows)
PreprocessorFactory.register("mask_genre_words", MaskGenreWords)
PreprocessorFactory.register("mask_genre", MaskGenreWords)
PreprocessorFactory.register("remove_duplicates", RemoveDuplicates)
PreprocessorFactory.register("dedupe", RemoveDuplicates)
PreprocessorFactory.register("remove_urls", RemoveURLs)
PreprocessorFactory.register("removeurls", RemoveURLs)
PreprocessorFactory.register("remove_repeated_characters", RemoveRepeatedCharacters)
PreprocessorFactory.register("remove_repeated", RemoveRepeatedCharacters)
PreprocessorFactory.register("remove_punctuation_noise", RemovePunctuationNoise)
PreprocessorFactory.register("punctuation_noise", RemovePunctuationNoise)
PreprocessorFactory.register("remove_whitespace", RemoveWhitespace)
PreprocessorFactory.register("whitespace", RemoveWhitespace)
PreprocessorFactory.register("merge_features", MergeFeatures)
PreprocessorFactory.register("merge", MergeFeatures)
PreprocessorFactory.register("explode_columns", ExplodeColumns)
PreprocessorFactory.register("explode", ExplodeColumns)
PreprocessorFactory.register("remove_html_tags", RemoveHTMLTags)
PreprocessorFactory.register("removehtml", RemoveHTMLTags)
PreprocessorFactory.register("log_transform", LogTransform)
PreprocessorFactory.register("log", LogTransform)
PreprocessorFactory.register("temporal_features", TemporalFeatures)
PreprocessorFactory.register("temporal", TemporalFeatures)
PreprocessorFactory.register("count_features", CountFeatures)
PreprocessorFactory.register("count", CountFeatures)
PreprocessorFactory.register("catalog_count", CatalogCount)
PreprocessorFactory.register("catalog", CatalogCount)
PreprocessorFactory.register("normalise_feature", NormaliseFeature)
PreprocessorFactory.register("normalise", NormaliseFeature)
PreprocessorFactory.register("cyclic_encode", CyclicEncode)
PreprocessorFactory.register("cyclic", CyclicEncode)

# List of required resources
REQUIRED_NLTK_RESOURCES = ["punkt", "stopwords"]

for res in REQUIRED_NLTK_RESOURCES:
    try:
        # Check if resource exists
        nltk.data.find(f"tokenizers/{res}" if res == "punkt" else f"corpora/{res}")
    except LookupError:
        # Download silently if missing
        nltk.download(res, quiet=True)

__all__ = ["Preprocessor", "PreprocessorFactory", "Stemmer", "Lemmatizer",
           "Lowercase", "StopwordRemover", "EmojiRemover", "FilterRows", "MaskGenreWords",
           "RemoveDuplicates", "RemoveURLs", "RemoveRepeatedCharacters", "RemovePunctuationNoise",
           "RemoveWhitespace", "MergeFeatures", "CountFeatures", "ExplodeColumns", "RemoveHTMLTags",
           "LogTransform", "TemporalFeatures", "CatalogCount", "NormaliseFeature", "CyclicEncode"]
