import os
import matplotlib.pyplot as plt
import pandas as pd

from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory


class GroupCountDistributionEDA(EDAComponent):
    """
    Generic EDA component for analysing the distribution of grouped counts.

    This component groups a dataframe by one column and counts the number of
    occurrences of another column within each group.

    Examples
    --------
    Comments per discussion:
        value_column="CommentID"
        group_by_column="DiscussionID"

    Conversation segments per discussion:
        value_column="ConversationSegmentID"
        group_by_column="DiscussionID"

    Comments per user:
        value_column="CommentID"
        group_by_column="UserID"
    """

    def __init__(
        self,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.logger = get_logger(self.__class__.__name__)
        self.visualisation_factory = VisualisationFactory()
        self.value_column = None
        self.group_by_column = None


    def run(
        self,
        data,
        target=None,
        text_field=None,
        save_path=None,
        **kwargs
    ):

        self.value_column = kwargs.get("value_column")
        self.group_by_column = kwargs.get("group_by_column")

        self.logger.info(
            "Starting GroupCountDistributionEDA"
        )

        # ---------------------------------------------------------
        # Validate required columns
        # ---------------------------------------------------------

        required_columns = [
            self.value_column,
            self.group_by_column
        ]

        missing_columns = [
            col
            for col in required_columns
            if col not in data.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {missing_columns}"
            )

        # ---------------------------------------------------------
        # Calculate grouped counts
        # ---------------------------------------------------------

        self.logger.info(
            f"Grouping by '{self.group_by_column}' "
            f"and counting '{self.value_column}'"
        )

        aggregated_data = (
            data
            .groupby(self.group_by_column)[self.value_column]
            .count()
            .reset_index(name="Count")
        )

        self.logger.info(
            f"Created grouped dataset containing "
            f"{len(aggregated_data):,} groups"
        )

        # ---------------------------------------------------------
        # Summary statistics
        # ---------------------------------------------------------

        stats = aggregated_data["Count"].describe()

        self.logger.info(
            "Distribution summary:\n%s",
            stats.to_string()
        )

        # ---------------------------------------------------------
        # Generate visualisations
        # ---------------------------------------------------------

        viz_params = kwargs.get("viz_params", [])

        for viz in viz_params:

            viz_name = viz["name"]

            viz_config = {
                k: v
                for k, v in viz.items()
                if k not in ["name", "filename"]
            }

            filename = viz.get(
                "filename",
                f"{viz_name}.png"
            )

            self.logger.info(
                f"Preparing visualisation '{viz_name}' "
                f"with config: {viz_config}"
            )

            visualisation = (
                self.visualisation_factory.get_visualisation(
                    viz_name,
                    **viz_config
                )
            )

            fig, ax = visualisation.plot(
                aggregated_data["Count"]
            )

            if filename and save_path:

                os.makedirs(
                    save_path,
                    exist_ok=True
                )

                output_file = os.path.join(
                    save_path,
                    filename
                )

                fig.savefig(
                    output_file,
                    bbox_inches="tight"
                )

                self.logger.info(
                    f"Saved visualisation to {output_file}"
                )

            plt.close(fig)

        self.logger.info(
            "Completed GroupCountDistributionEDA successfully"
        )

        return aggregated_data