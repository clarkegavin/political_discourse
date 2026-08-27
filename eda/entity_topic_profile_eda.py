import os
import pandas as pd

from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory


class EntityTopicProfileEDA(EDAComponent):
    """
    Analyse LLM-generated thematic profiles for entities.

    The unit of analysis is:

        Entity × TopicTheme

    For each configured entity attribute, this component calculates:

    - total questions associated with each entity
    - number of distinct LLM-generated themes per entity
    - number of questions associated with each entity/theme
    - topic share:
          questions on theme / total questions by entity
    - rank of themes within each entity

    The top N entities are selected by overall question volume,
    and the top N themes are retained for each selected entity.
    """

    def __init__(
        self,
        topic_field="Topic",
        topic_theme_field="TopicTheme",
        attributes=None,
        top_n_topics=5,
        top_n_entities=10,
        **kwargs
    ):
        self.logger = get_logger(
            self.__class__.__name__
        )

        self.topic_field = topic_field
        self.topic_theme_field = topic_theme_field
        self.attributes = attributes or []

        self.top_n_topics = top_n_topics
        self.top_n_entities = top_n_entities

        self.visualisation_factory = (
            VisualisationFactory()
        )

        self.logger.info(
            f"Initialized EntityTopicProfileEDA "
            f"with topic_field={topic_field}, "
            f"topic_theme_field={topic_theme_field}, "
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

        # --------------------------------------------------
        # Validate columns
        # --------------------------------------------------

        required_columns = (
            list(attributes)
            + [
                topic_field,
                topic_theme_field
            ]
        )

        missing_columns = [
            column
            for column in required_columns
            if column not in data.columns
        ]

        if missing_columns:
            raise ValueError(
                "Missing required columns: "
                f"{missing_columns}"
            )

        # Do not modify the original dataframe.
        working_data = data.copy()

        results = {}

        # --------------------------------------------------
        # Process each entity attribute
        # --------------------------------------------------

        for attribute in attributes:

            self.logger.info(
                f"Processing attribute '{attribute}'"
            )

            # --------------------------------------------------
            # Entity totals
            #
            # IMPORTANT:
            # This uses ALL questions for the entity, including
            # questions where TopicTheme is null.
            # --------------------------------------------------

            entity_totals = (
                working_data[
                    working_data[attribute].notna()
                ]
                .groupby(attribute)
                .size()
                .reset_index(
                    name="total_questions"
                )
            )

            # --------------------------------------------------
            # Only questions with an LLM-generated theme can
            # contribute to the thematic profile.
            # --------------------------------------------------

            topic_data = (
                working_data[
                    working_data[attribute].notna()
                    & working_data[topic_theme_field].notna()
                ]
                .copy()
            )

            if topic_data.empty:

                self.logger.warning(
                    f"No TopicTheme data found for "
                    f"attribute '{attribute}'"
                )

                results[attribute] = {
                    "data": pd.DataFrame(),
                    "entity_summary": pd.DataFrame(),
                    "top_entities": [],
                    "topic_theme_field": topic_theme_field,
                }

                continue

            # --------------------------------------------------
            # Entity × TopicTheme counts
            #
            # TopicTheme is deliberately the unit of analysis.
            # --------------------------------------------------

            profile_data = (
                topic_data
                .groupby(
                    [
                        attribute,
                        topic_theme_field
                    ]
                )
                .size()
                .reset_index(
                    name="topic_count"
                )
            )

            # --------------------------------------------------
            # Add total entity questions
            # --------------------------------------------------

            profile_data = (
                profile_data
                .merge(
                    entity_totals,
                    on=attribute,
                    how="left"
                )
            )

            # --------------------------------------------------
            # Topic share
            #
            # Number of questions on theme /
            # total questions by entity
            # --------------------------------------------------

            profile_data["topic_share"] = (
                profile_data["topic_count"]
                / profile_data["total_questions"]
            )

            # --------------------------------------------------
            # Rank themes within each entity
            # --------------------------------------------------

            profile_data["topic_rank"] = (
                profile_data
                .groupby(attribute)["topic_count"]
                .rank(
                    method="first",
                    ascending=False
                )
                .astype(int)
            )

            # --------------------------------------------------
            # Distinct LLM themes per entity
            # --------------------------------------------------

            entity_theme_counts = (
                profile_data
                .groupby(attribute)
                .size()
                .reset_index(
                    name="distinct_themes"
                )
            )

            # --------------------------------------------------
            # Entity summary
            # --------------------------------------------------

            entity_summary = (
                entity_totals
                .merge(
                    entity_theme_counts,
                    on=attribute,
                    how="left"
                )
            )

            entity_summary["distinct_themes"] = (
                entity_summary["distinct_themes"]
                .fillna(0)
                .astype(int)
            )

            # --------------------------------------------------
            # Select top entities by question volume
            # --------------------------------------------------

            top_entities_df = (
                entity_summary
                .sort_values(
                    [
                        "total_questions",
                        attribute
                    ],
                    ascending=[
                        False,
                        True
                    ]
                )
                .head(top_n_entities)
                .copy()
            )

            top_entities = (
                top_entities_df[
                    attribute
                ]
                .tolist()
            )

            # --------------------------------------------------
            # Retain selected entities and their top themes
            # --------------------------------------------------

            profile_data = (
                profile_data[
                    profile_data[attribute].isin(
                        top_entities
                    )
                    & (
                        profile_data["topic_rank"]
                        <= top_n_topics
                    )
                ]
                .copy()
            )

            # --------------------------------------------------
            # Add distinct theme count to profile data
            # --------------------------------------------------

            profile_data = (
                profile_data
                .merge(
                    entity_summary[
                        [
                            attribute,
                            "distinct_themes"
                        ]
                    ],
                    on=attribute,
                    how="left"
                )
            )

            # --------------------------------------------------
            # Entity ordering
            #
            # Highest question-volume entity first.
            # --------------------------------------------------

            entity_order = (
                top_entities_df[
                    attribute
                ]
                .tolist()
            )

            profile_data[attribute] = pd.Categorical(
                profile_data[attribute],
                categories=entity_order,
                ordered=True
            )

            profile_data = (
                profile_data
                .sort_values(
                    [
                        attribute,
                        "topic_rank"
                    ]
                )
                .reset_index(drop=True)
            )

            # --------------------------------------------------
            # Logging
            # --------------------------------------------------

            self.logger.info(
                f"Attribute '{attribute}': "
                f"{len(entity_summary)} total entities; "
                f"{len(top_entities)} selected"
            )

            self.logger.info(
                f"Top entities: {top_entities}"
            )

            # --------------------------------------------------
            # Store results
            # --------------------------------------------------

            results[attribute] = {
                "data": profile_data,
                "entity_summary": top_entities_df,
                "top_entities": top_entities,
                "topic_theme_field": topic_theme_field,
            }

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
                key: value
                for key, value in viz.items()
                if key not in [
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

            figures = visualisation.plot(
                results,
                **viz_config
            )

            # --------------------------------------------------
            # Visualisation may return:
            #
            #   {attribute: figure}
            #
            # or:
            #
            #   figure
            # --------------------------------------------------

            if isinstance(figures, dict):

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

                    self.logger.info(
                        f"Saved '{attribute}' "
                        f"visualisation to "
                        f"{output_path}"
                    )

            else:

                output_path = os.path.join(
                    save_path,
                    filename
                )

                figures.savefig(
                    output_path,
                    dpi=300,
                    bbox_inches="tight"
                )

                self.logger.info(
                    f"Saved visualisation to "
                    f"{output_path}"
                )

        return results