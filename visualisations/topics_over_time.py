# visualisations/topics_over_time.py

from .bertopic_visualisation import BERTopicVisualisation

import pandas as pd


class TopicsOverTime(BERTopicVisualisation):
    """
    BERTopic topics over time visualisation.

    Requires:
      - text_column: column containing topic representation documents
      - date_column: column containing timestamps

    Optional:
      - interval:
            None      -> BERTopic default binning
            week
            month
            quarter
            year
    """

    def __init__(
        self,
        output_dir=".",
        filename="topics_over_time.html",
        date_column=None,
        text_column=None,
        interval=None,
        **params
    ):

        super().__init__(
            name="topics_over_time",
            #title="Topics Over Time",
            output_dir=output_dir,
            filename=filename,
            method="topics_over_time",
            date_column=date_column,
            text_column=text_column,
            interval=interval,
            **params
        )

        self.date_column = date_column
        self.text_column = text_column
        self.interval = interval


    def _apply_interval(
        self,
        timestamps
    ):
        """
        Convert timestamps according to configured interval.

        If interval is None, return original timestamps
        and allow BERTopic to determine bins.
        """

        if self.interval is None:
            return timestamps


        timestamps = pd.to_datetime(
            timestamps
        )


        if self.interval == "week":

            return (
                timestamps
                .dt
                .to_period("W")
                .astype(str)
                .tolist()
            )


        if self.interval == "month":

            return (
                timestamps
                .dt
                .to_period("M")
                .astype(str)
                .tolist()
            )


        if self.interval == "quarter":

            return (
                timestamps
                .dt
                .to_period("Q")
                .astype(str)
                .tolist()
            )


        if self.interval == "year":

            return (
                timestamps
                .dt
                .year
                .astype(str)
                .tolist()
            )


        self.logger.warning(
            f"Unknown interval '{self.interval}'. "
            "Using original timestamps."
        )

        return timestamps.tolist()



    def plot(
        self,
        data,
        model=None,
        metadata=None,
        save_path=None,
        filename=None,
        **plot_kwargs
    ):

        if data is None:
            self.logger.warning(
                "No dataframe supplied for topics over time"
            )
            return None


        if model is None:
            self.logger.warning(
                "No model supplied for topics over time"
            )
            return None


        if not self.date_column:
            self.logger.warning(
                "No date_column supplied"
            )
            return None


        if not self.text_column:
            self.logger.warning(
                "No text_column supplied"
            )
            return None


        if self.date_column not in data.columns:

            self.logger.warning(
                f"Date column '{self.date_column}' "
                "not found in dataframe"
            )

            return None


        if self.text_column not in data.columns:

            self.logger.warning(
                f"Text column '{self.text_column}' "
                "not found in dataframe"
            )

            return None


        bertopic_model = self._get_bertopic_model(
            model
        )


        if bertopic_model is None:
            return None


        try:

            docs = (
                data[self.text_column]
                .fillna("")
                .tolist()
            )


            timestamps = self._apply_interval(
                data[self.date_column]
            )


            self.logger.info(
                f"Generating topics over time using "
                f"{len(docs)} documents"
            )


            topics_over_time = (
                bertopic_model
                .topics_over_time(
                    docs,
                    timestamps
                )
            )


            fig = (
                bertopic_model
                .visualize_topics_over_time(
                    topics_over_time,
                    **plot_kwargs
                )
            )


        except Exception as e:

            self.logger.exception(
                f"Failed generating topics over time: {e}"
            )

            return None


        return self._save(
            fig,
            save_path,
            filename
        )