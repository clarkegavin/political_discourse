import os
import matplotlib.pyplot as plt
import pandas as pd

from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory


class ReplyChainAnalysisEDA(EDAComponent):
    """
    Analyse reply chains by counting the number of referenced reply IDs
    per comment and grouping into:
        - 0 reply IDs
        - 1 reply ID
        - 2+ reply IDs
    """

    def __init__(self, column="ReplyToIDs", **kwargs):
        super().__init__(**kwargs)

        self.logger = get_logger(self.__class__.__name__)
        self.column = column
        self.visualisation_factory = VisualisationFactory()

        self.logger.info(
            f"Initialized ReplyChainAnalysisEDA with column={column}"
        )


    def run(
        self,
        data,
        target=None,
        text_field=None,
        save_path=None,
        **kwargs
    ):

        self.logger.info(
            "Starting reply chain analysis"
        )

        if self.column not in data.columns:
            raise ValueError(
                f"Column '{self.column}' not found in dataframe"
            )

        # ---------------------------------------------------------
        # Calculate number of reply IDs per comment
        # ---------------------------------------------------------

        def count_reply_ids(value):

            if value is None:
                return 0

            if isinstance(value, list):
                return len(value)

            # Handle possible NaN values
            if pd.isna(value):
                return 0

            # Fallback if stored as string representation
            if isinstance(value, str):

                if value.strip() in ["", "[]"]:
                    return 0

                try:
                    import ast
                    parsed = ast.literal_eval(value)

                    if isinstance(parsed, list):
                        return len(parsed)

                except Exception:
                    pass

            return 0


        reply_counts = data[self.column].apply(count_reply_ids)


        # ---------------------------------------------------------
        # Bucket reply counts
        # ---------------------------------------------------------

        buckets = pd.cut(
            reply_counts,
            bins=[-1, 0, 1, float("inf")],
            labels=[
                "0 Explicit Replies",
                "1 Explicit Replies",
                "2+ Explicit Replies"
            ]
        )


        aggregated_data = (
            buckets
            .value_counts()
            .reindex(
                [
                    "0 Explicit Replies",
                    "1 Explicit Replies",
                    "2+ Explicit Replies"
                ],
                fill_value=0
            )
        )


        self.logger.info(
            f"Reply chain distribution:\n{aggregated_data}"
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
                "reply_chain_analysis.png"
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
                aggregated_data.to_dict()
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


        return aggregated_data