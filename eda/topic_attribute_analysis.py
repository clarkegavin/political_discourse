# eda/topic_attribute_analysis_eda.py

import os
import pandas as pd
import matplotlib.pyplot as plt

from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory


class TopicAttributeAnalysisEDA(EDAComponent):

    def __init__(
        self,
        topic_field="Topic",
        topic_theme_field="TopicTheme",
        attributes=None,
        top_n_topics=10,
        top_n_entities=5,
        **kwargs
    ):
        self.logger = get_logger(self.__class__.__name__)

        self.topic_field = topic_field
        self.topic_theme_field = topic_theme_field
        self.attributes = attributes or []
        self.top_n_topics = top_n_topics
        self.top_n_entities = top_n_entities

        self.visualisation_factory = VisualisationFactory()

        self.logger.info(
            f"Initialized TopicAttributeAnalysisEDA "
            f"with topic_field={topic_field}, "
            f"attributes={self.attributes}, "
            f"top_n_topics={top_n_topics}, "
            f"top_n_entities={top_n_entities}"
        )

    def run(
        self,
        data,
        target=None,
        text_field=None,
        save_path=None,
        **kwargs
    ):


        if save_path is None:
            save_path = os.getcwd()

        topic_field = kwargs.get(
            "topic_field",
            self.topic_field
        )

        topic_theme_field = kwargs.get(
            "topic_theme_field",
            self.topic_theme_field
        )

        attributes = kwargs.get(
            "attributes",
            self.attributes
        )

        top_n_topics = kwargs.get(
            "top_n_topics",
            self.top_n_topics
        )

        top_n_entities = kwargs.get(
            "top_n_entities",
            self.top_n_entities
        )

        self.logger.info(
            f"Incoming dataframe shape: {data.shape}"
        )

        self.logger.info(
            data[
                [
                    topic_field,
                    topic_theme_field,
                    "Questioner"
                ]
            ]
            .query(f"{topic_field} == 0")
            .head(20)
            .to_string()
        )

        self.logger.info(
            "Topic 0 unique Questioners: "
            + str(
                data.loc[
                    data[topic_field] == 0,
                    "Questioner"
                ].nunique()
            )
        )

        # --------------------------------------------------
        # Validation
        # --------------------------------------------------

        required_columns = [
            topic_field,
            topic_theme_field,
            *attributes
        ]

        missing = [
            column
            for column in required_columns
            if column not in data.columns
        ]

        if missing:
            raise ValueError(
                f"Required columns not found in data: {missing}"
            )

        # Do not modify the original dataframe
        working_data = data.copy()

        # --------------------------------------------------
        # Process each attribute
        # --------------------------------------------------

        results = {}

        for attribute in attributes:

            self.logger.info(
                f"Processing topic analysis for attribute '{attribute}'"
            )

            attribute_data = (
                working_data[
                    [
                        topic_field,
                        topic_theme_field,
                        attribute
                    ]
                ]
                .dropna(subset=[topic_field, attribute])
            )

            # Count documents/questions for each
            # topic × attribute combination
            grouped = (
                attribute_data
                .groupby(
                    [
                        topic_field,
                        topic_theme_field,
                        attribute
                    ],
                    dropna=False
                )
                .size()
                .reset_index(name="count")
            )

            self.logger.info(
                grouped[
                    grouped[topic_field] == 0
                    ]
                .sort_values("count", ascending=False)
                .head(20)
                .to_string()
            )

            self.logger.info(
                grouped[
                    grouped[topic_field] == 0
                    ]["count"].sum()
            )

            # Determine the globally most prevalent topics
            # for this attribute
            topic_totals = (
                grouped
                .groupby(
                    [topic_field, topic_theme_field],
                    as_index=False
                )["count"]
                .sum()
                .sort_values(
                    "count",
                    ascending=False
                )
                .head(top_n_topics)
            )

            top_topics = topic_totals[topic_field].tolist()

            filtered = grouped[
                grouped[topic_field].isin(top_topics)
            ].copy()

            # Retain the topic ordering
            topic_order = (
                topic_totals
                .sort_values("count", ascending=False)
                [topic_field]
                .tolist()
            )

            filtered["_topic_order"] = pd.Categorical(
                filtered[topic_field],
                categories=topic_order,
                ordered=True
            )

            filtered = filtered.sort_values(
                "_topic_order"
            )

            results[attribute] = {
                "data": filtered,
                "topic_totals": topic_totals,
                "top_topics": top_topics
            }

            self.logger.info(
                filtered[
                    [
                        topic_field,
                        topic_theme_field,
                        attribute,
                        "count"
                    ]
                ].head(20).to_string()
            )

        # --------------------------------------------------
        # Visualisations
        # --------------------------------------------------

        viz_params = kwargs.get(
            "viz_params",
            []
        )

        for viz in viz_params:

            viz_name = viz["name"]

            viz_config = {
                k: v
                for k, v in viz.items()
                if k not in [
                    "name",
                    "filename"
                ]
            }

            filename = viz.get(
                "filename",
                f"{viz_name}.png"
            )

            visualisation = (
                self.visualisation_factory
                .get_visualisation(
                    viz_name,
                    **viz_config
                )
            )

            if visualisation is None:
                raise ValueError(
                    f"Visualisation '{viz_name}' "
                    f"not found in VisualisationFactory"
                )

            # This visualisation handles all requested
            # attributes and creates one figure per attribute.
            figures = visualisation.plot(
                results,
                top_n_entities=top_n_entities
            )

            for attribute, fig in figures.items():

                attribute_filename = (
                    filename
                    .replace(
                        ".png",
                        f"_{attribute}.png"
                    )
                )

                output_path = os.path.join(
                    save_path,
                    attribute_filename
                )

                fig.savefig(
                    output_path,
                    dpi=300,
                    bbox_inches="tight"
                )

                plt.close(fig)

                self.logger.info(
                    f"Saved '{attribute}' visualisation to "
                    f"{output_path}"
                )

        return results