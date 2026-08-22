import os

import pandas as pd
import matplotlib.pyplot as plt

from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory


class TopicSimilarityAlignmentEDA(EDAComponent):
    """
    Analyse engagement with topics that are also discussed
    in the online forum.

    The unit of analysis is the parliamentary question.

    Metrics calculated for each grouping entity:

        TotalQuestions
            Total number of parliamentary questions raised.

        MatchedQuestions
            Number of parliamentary questions whose topic has
            been matched to one or more online forum topics.

        QuestionMatchPercentage
            Percentage of questions raised on matched topics.

    The grouping field is configurable, allowing this EDA to be
    reused for:

        Questioner
        QuestionerParty
        QuestionerConstituency
    """

    def __init__(
        self,
        group_field="Questioner",
        topic_field="Topic",
        match_field="IsMatched",
        min_matched_questions=1,
        **kwargs
    ):

        self.logger = get_logger(
            self.__class__.__name__
        )

        self.group_field = group_field
        self.topic_field = topic_field
        self.match_field = match_field
        self.min_matched_questions = min_matched_questions

        self.visualisation_factory = VisualisationFactory()

        self.logger.info(
            "Initialized TopicSimilarityAlignmentEDA "
            f"with group_field={group_field}, "
            f"topic_field={topic_field}, "
            f"match_field={match_field}, "
            f"min_matched_questions={min_matched_questions}"
        )

    # ------------------------------------------------------------------
    # Main execution
    # ------------------------------------------------------------------

    def run(
        self,
        data,
        target=None,
        text_field=None,
        save_path=None,
        **kwargs
    ):

        if data is None:
            raise ValueError(
                "Data must be provided to "
                "TopicSimilarityAlignmentEDA."
            )

        if save_path is None:
            save_path = os.getcwd()

        os.makedirs(
            save_path,
            exist_ok=True
        )

        # --------------------------------------------------------------
        # Resolve configuration
        # --------------------------------------------------------------

        group_field = kwargs.get(
            "group_field",
            self.group_field
        )

        topic_field = kwargs.get(
            "topic_field",
            self.topic_field
        )

        match_field = kwargs.get(
            "match_field",
            self.match_field
        )

        min_matched_questions = kwargs.get(
            "min_matched_questions",
            self.min_matched_questions
        )

        # --------------------------------------------------------------
        # Validate configuration
        # --------------------------------------------------------------

        if min_matched_questions < 1:
            raise ValueError(
                "min_matched_questions must be greater than zero."
            )

        required_columns = [
            group_field,
            topic_field,
            match_field
        ]

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

        # --------------------------------------------------------------
        # Working data
        #
        # IMPORTANT:
        #
        # Do not remove Topic == -1.
        #
        # Outlier questions are still parliamentary questions and
        # therefore must contribute to TotalQuestions.
        # --------------------------------------------------------------

        working_data = data.copy()

        self.logger.info(
            f"Input data contains "
            f"{len(working_data)} parliamentary questions"
        )

        # --------------------------------------------------------------
        # Total questions
        #
        # Count every question, including Topic == -1.
        # --------------------------------------------------------------

        total_questions = (
            working_data
            .groupby(group_field)
            .size()
            .rename("TotalQuestions")
        )

        # --------------------------------------------------------------
        # Matched questions
        #
        # Count rows where IsMatched == 1.
        #
        # This intentionally counts repeated questions on the same
        # topic separately.
        # --------------------------------------------------------------

        matched_questions = (
            working_data[
                working_data[match_field] == 1
            ]
            .groupby(group_field)
            .size()
            .rename("MatchedQuestions")
        )

        # --------------------------------------------------------------
        # Combine results
        # --------------------------------------------------------------

        results = pd.concat(
            [
                total_questions,
                matched_questions
            ],
            axis=1
        )

        results["MatchedQuestions"] = (
            results["MatchedQuestions"]
            .fillna(0)
            .astype(int)
        )

        results["TotalQuestions"] = (
            results["TotalQuestions"]
            .astype(int)
        )

        # --------------------------------------------------------------
        # Calculate percentage
        # --------------------------------------------------------------

        results["QuestionMatchPercentage"] = (
            results["MatchedQuestions"]
            / results["TotalQuestions"]
            * 100
        )

        # --------------------------------------------------------------
        # Filter groups
        #
        # Only retain groups with at least the configured number
        # of matched questions.
        # --------------------------------------------------------------

        results = results[
            results["MatchedQuestions"]
            >= min_matched_questions
        ].copy()

        # --------------------------------------------------------------
        # Sort by percentage
        # --------------------------------------------------------------

        results = results.sort_values(
            "QuestionMatchPercentage",
            ascending=False
        )

        # --------------------------------------------------------------
        # Reset index so group_field becomes a normal column
        # --------------------------------------------------------------

        results = results.reset_index()

        # --------------------------------------------------------------
        # Logging
        # --------------------------------------------------------------

        self.logger.info(
            f"Total groups before filtering: "
            f"{len(total_questions)}"
        )

        self.logger.info(
            f"Groups after minimum matched-question filter: "
            f"{len(results)}"
        )

        self.logger.info(
            f"Minimum matched questions: "
            f"{min_matched_questions}"
        )

        # --------------------------------------------------------------
        # Visualisations
        # --------------------------------------------------------------

        viz_params = kwargs.get(
            "viz_params",
            []
        )

        for viz in viz_params:

            viz_name = viz["name"]

            filename = viz.get(
                "filename",
                f"{viz_name}.png"
            )

            # ----------------------------------------------------------
            # Visualisation configuration
            #
            # All visualisation parameters are defined directly under
            # the YAML visualisation entry.
            # ----------------------------------------------------------

            viz_config = {
                key: value
                for key, value in viz.items()
                if key not in [
                    "name",
                    "filename"
                ]
            }

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

            # ----------------------------------------------------------
            # Plot
            #
            # Configuration has already been supplied to the
            # visualisation constructor.
            # ----------------------------------------------------------

            fig, ax = visualisation.plot(
                data=results
            )

            # ----------------------------------------------------------
            # Save
            # ----------------------------------------------------------

            output_path = os.path.join(
                save_path,
                filename
            )

            fig.savefig(
                output_path,
                dpi=300,
                bbox_inches="tight"
            )

            plt.close(fig)

            self.logger.info(
                f"Saved visualisation to {output_path}"
            )

        # --------------------------------------------------------------
        # Return aggregated results
        # --------------------------------------------------------------

        return results