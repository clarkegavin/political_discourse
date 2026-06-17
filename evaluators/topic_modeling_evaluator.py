# evaluators/topic_modeling_evaluator.py
import numpy as np
from logs.logger import get_logger
from gensim.models.coherencemodel import CoherenceModel
from sklearn.feature_extraction.text import CountVectorizer
from typing import List
from .base import Evaluator
from gensim.corpora import Dictionary
from collections import Counter


class TopicModelingEvaluator(Evaluator):
    """Evaluator for topic models. Computes coherence, diversity and topic sizes."""

    def __init__(self, name: str = "topic_modeling", coherence_type: str = "c_v", top_n: int = 10, **kwargs):
        # initialize base evaluator (sets base logger and stores kwargs)
        super().__init__(name, **kwargs)
        # class-specific settings
        self.coherence_type = coherence_type
        self.top_n = top_n
        # override logger with class-specific name for clearer logs
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info(f"Initialized TopicModelingEvaluator(coherence_type={coherence_type}, top_n={top_n})")
        self.combined_text_field_name = kwargs.get("combined_text_field_name", "__topic_input_text__")
        self.logger.info(f"Top N terms for evaluation: {self.top_n}")

    def _compute_exclusivity(self, top_terms: List[List[str]]) -> float:
        """Compute topic exclusivity as: 1 - (number of shared top words across topics / total number of top words).
        - Each topic's top words are treated as a set to avoid double counting within the same topic.
        - Returns 0.0 for empty inputs.
        """
        if not top_terms:
            return 0.0

        # Create per-topic sets and filter out empty topics
        topic_sets = [set(t) for t in top_terms if t]
        if not topic_sets:
            return 0.0

        # Total number of top words (counting each topic's unique words once per topic)
        total_top_words = sum(len(s) for s in topic_sets)
        if total_top_words == 0:
            return 0.0

        # Count in how many topics each word appears
        word_counts = Counter()
        for s in topic_sets:
            word_counts.update(s)

        # Number of unique words that appear in more than one topic
        shared_unique_count = sum(1 for _, c in word_counts.items() if c > 1)

        exclusivity = 1.0 - (shared_unique_count / float(total_top_words))
        # Clamp to [0,1]
        return float(max(0.0, min(1.0, exclusivity)))

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
        self.logger.info("Evaluating topic model assignments")
        self.logger.info(f"Model type received in evaluator: {type(model)}")
        #estimator = model.estimator
        if isinstance(model, dict):
            estimator = model.get("model") or model.get("estimator")
        else:
            estimator = getattr(model, "estimator", model)

        docs = X[self.combined_text_field_name].fillna("").tolist()
        metrics = {}

        self.logger.info(f"Number of documents: {len(docs)}, Number of topic assignments: {len(topics)}")
        # Topic sizes
        unique, counts = np.unique(topics, return_counts=True)
        sizes = dict(zip(unique.tolist(), counts.tolist()))
        metrics["topic_sizes"] = sizes

        self.logger.info(f"Topic sizes: {sizes}")
        # Topic diversity: proportion of unique top terms across topics
        top_terms = self._extract_top_terms(estimator, self.top_n)
        flat = [t for terms in top_terms for t in terms]
        if flat:
            metrics["topic_diversity"] = float(len(set(flat)) / max(1, len(flat)))
        else:
            metrics["topic_diversity"] = 0.0

        self.logger.info(f"Extracted top terms for diversity calculation: {flat}")
        # Topic coherence using gensim (c_v recommended) when we can extract topic terms
        if top_terms:
            try:
                self.logger.info("Computing topic coherence using gensim")
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
                    self.logger.info(f"Computing coherence with {len(topics_for_gensim)} topics and {len(tokenized)} documents")
                    cm = CoherenceModel(topics=topics_for_gensim, texts=tokenized, dictionary=dictionary, coherence=self.coherence_type, processes=1)
                    self.logger.info("Coherence model computed successfully, extracting coherence score")
                    coherence = cm.get_coherence()
                    self.logger.info(f"Computed coherence: {coherence}")
                    metrics["coherence"] = float(coherence)

                    # Additionally compute NPMI coherence (c_npmi) and add as separate metric
                    try:
                        self.logger.info("Computing NPMI coherence (c_npmi) using gensim")
                        cm_npmi = CoherenceModel(topics=topics_for_gensim, texts=tokenized, dictionary=dictionary, coherence="c_npmi", processes=1)
                        coherence_npmi = cm_npmi.get_coherence()
                        self.logger.info(f"Computed coherence (c_npmi): {coherence_npmi}")
                        metrics["coherence_npmi"] = float(coherence_npmi)
                    except Exception as e:
                        self.logger.warning(f"Could not compute coherence (c_npmi): {e}")
            except Exception as e:
                self.logger.warning(f"Could not compute coherence: {e}")

        # Compute topic exclusivity
        exclusivity = self._compute_exclusivity(top_terms)
        metrics["topic_exclusivity"] = exclusivity
        self.logger.info(f"Computed topic exclusivity: {exclusivity}")

        # Compute outlier ratio: proportion of documents assigned to the outlier topic (-1)
        if len(topics) > 0:
            outlier_ratio = (np.sum(np.array(topics) == -1) / len(topics))
        else:
            outlier_ratio = 0.0
        metrics["outlier_ratio"] = outlier_ratio
        self.logger.info(f"Computed outlier ratio: {outlier_ratio}")

        return metrics
