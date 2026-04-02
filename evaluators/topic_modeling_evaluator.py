# evaluators/topic_modeling_evaluator.py
import numpy as np
from logs.logger import get_logger
from gensim.models.coherencemodel import CoherenceModel
from sklearn.feature_extraction.text import CountVectorizer
from typing import List
from .base import Evaluator
from gensim.corpora import Dictionary


logger = get_logger("TopicModelingEvaluator")

class TopicModelingEvaluator(Evaluator):
    """Evaluator for topic models. Computes coherence, diversity and topic sizes."""

    def __init__(self, name: str = "topic_modeling", coherence_type: str = "c_v", top_n: int = 10, **kwargs):
        self.name = name
        self.coherence_type = coherence_type
        self.top_n = top_n
        self.logger = logger

    # def _extract_top_terms(self, model, top_n):
    #     # model may be BERTopic (has get_topic) or sklearn (has components_)
    #     topics_terms = []
    #     if hasattr(model, "get_topic"):
    #         # BERTopic: topics are keyed by integer, get_topic returns list of (term, score)
    #         i = 0
    #         while True:
    #             try:
    #                 terms = model.get_topic(i)
    #                 if not terms:
    #                     break
    #                 topics_terms.append([t for t, _ in terms][:top_n])
    #                 i += 1
    #             except Exception:
    #                 break
    #     elif hasattr(model, "components_"):
    #         # sklearn topic model
    #         try:
    #             # Need vectorizer to map indices back to terms - not available here, so return placeholders
    #             n_topics = model.components_.shape[0]
    #             for i in range(n_topics):
    #                 comp = model.components_[i]
    #                 top_idx = np.argsort(comp)[-top_n:][::-1]
    #                 terms = [str(idx) for idx in top_idx]
    #                 topics_terms.append(terms)
    #         except Exception:
    #             pass
    #     return topics_terms

    def _extract_top_terms(self, estimator, top_n):

        topics_terms = []

        if hasattr(estimator, "get_topics"):  # BERTopic
            topics = estimator.get_topics()

            for topic_id, terms in topics.items():
                if topic_id == -1:  # ignore outliers
                    continue

                topics_terms.append([t for t, _ in terms][:top_n])

        elif hasattr(estimator, "components_"):  # sklearn LDA/NMF
            for comp in estimator.components_:
                top_idx = np.argsort(comp)[-top_n:][::-1]
                topics_terms.append([str(idx) for idx in top_idx])

        return topics_terms

    def evaluate(self, X, topics: List[int], model) -> dict:
        """Compute metrics for the provided assignments.

        Args:
          X: original DataFrame with __topic_input_text__
          topics: list/array of topic assignments
          model: trained model instance
        Returns:
          dict of metrics
        """
        estimator = model.estimator
        docs = X["__topic_input_text__"].fillna("").tolist()
        metrics = {}

        # Topic sizes
        unique, counts = np.unique(topics, return_counts=True)
        sizes = dict(zip(unique.tolist(), counts.tolist()))
        metrics["topic_sizes"] = sizes

        # Topic diversity: proportion of unique top terms across topics
        top_terms = self._extract_top_terms(estimator, self.top_n)
        flat = [t for terms in top_terms for t in terms]
        if flat:
            metrics["topic_diversity"] = float(len(set(flat)) / max(1, len(flat)))
        else:
            metrics["topic_diversity"] = 0.0

        # Topic coherence using gensim (c_v recommended) when we can extract topic terms
        if top_terms:
            try:
                # Build gensim objects
                vec = CountVectorizer()
                doc_term = vec.fit_transform(docs)
                feature_names = vec.get_feature_names_out()

                # convert top_terms words to indices in feature_names if possible
                topics_for_gensim = []
                for tlist in top_terms:
                    mapped = [w for w in tlist if w in feature_names]
                    if mapped:
                        topics_for_gensim.append(mapped)

                if topics_for_gensim:
                    tokenized = [d.split() for d in docs]
                    dictionary = Dictionary(tokenized)

                    cm = CoherenceModel(topics=topics_for_gensim, texts=tokenized, dictionary=dictionary, coherence=self.coherence_type)
                    coherence = cm.get_coherence()
                    metrics["coherence"] = float(coherence)
            except Exception as e:
                self.logger.warning(f"Could not compute coherence: {e}")

        return metrics

