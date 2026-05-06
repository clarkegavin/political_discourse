# tests/test_topic_modeling_experiment_unit.py
import pandas as pd
from models.factory import ModelFactory
from experiments.topic_modeling_experiment import TopicModelingExperiment


class FakeTopicModel:
    def __init__(self, name, **kwargs):
        self.name = name

    def fit_transform(self, docs, embeddings=None):
        # Return a topic id per doc as a simple list (non-BERTopic model path expects a 1-D output)
        topics = [0 for _ in docs]
        return topics

    def get_topic_info(self):
        return pd.DataFrame({"Topic": [0], "Name": ["topic0"], "Count": [len([1]) ]})

    def get_topics(self):
        return {0: [("word1", 1.0), ("word2", 0.5)]}


def test_topic_modeling_experiment_returns_structure(tmp_path):
    # Register fake model
    ModelFactory.register_model("fake_topic_model", FakeTopicModel)

    # Build small dataframe
    df = pd.DataFrame({"text": ["hello world", "another doc"]})
    # Provide combined_text_field_name pointing to 'text'
    exp = TopicModelingExperiment(
        name="tst",
        model_name="fake_topic_model",
        evaluator_name="dummy",
        X=df,
        combined_text_field_name="text",
        save_path=str(tmp_path)
    )

    result = exp.run()
    assert isinstance(result, dict)
    assert "df" in result and "metadata" in result and "artifacts" in result
    assert result["df"].shape[0] == 2
    assert result["artifacts"] == [str(tmp_path / "tst_topic_info.csv")] or result["artifacts"] == []
