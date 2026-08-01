import re
import os
import pandas as pd
import matplotlib.pyplot as plt

from eda.base import EDAComponent
from visualisations.factory import VisualisationFactory
from data.savers.factory import DataSaverFactory
from data.sqlalchemy_connector import SQLAlchemyConnector
from logs.logger import get_logger


class IdentifyURLsEDA(EDAComponent):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.logger = get_logger(self.__class__.__name__)
        self.visualisation_factory = VisualisationFactory()

        self.pattern = re.compile(
            r'(?i)\b(?:https?://|www\.)[^\s<>"\']+',
            flags=re.IGNORECASE
        )

        self.logger.info("Initialized IdentifyURLsEDA")


    def _count_urls(self, text):

        if pd.isna(text):
            return 0

        if not isinstance(text, str):
            text = str(text)

        return len(self.pattern.findall(text))


    def run(
        self,
        data: pd.DataFrame,
        target=None,
        text_field=None,
        save_path=None,
        **kwargs
    ):

        columns = kwargs.get("columns", [])
        viz_params = kwargs.get("viz_params", [])

        saver_name = kwargs.get("saver_name")
        table_name = kwargs.get("table_name")
        schema = kwargs.get("schema")
        if_exists = kwargs.get("if_exists", "fail")
        chunk_size = kwargs.get("chunk_size", 1000)
        connector_params = kwargs.get("connector_params", {})


        if save_path is None:
            save_path = os.getcwd()


        results = []


        for column in columns:

            if column not in data.columns:
                self.logger.warning(
                    f"Column {column} not found"
                )
                continue


            self.logger.info(
                f"Analysing URLs in column {column}"
            )


            for idx, text in data[column].items():

                url_count = self._count_urls(text)

                document_id = (
                    data.loc[idx, "DocumentId"]
                    if "DocumentId" in data.columns
                    else None
                )


                results.append(
                    {
                        "DocumentID": document_id,
                        "RecordID": idx,
                        "ColumnName": column,
                        "URLCount": url_count,
                        "ContainsURL": url_count > 0
                    }
                )


        result_df = pd.DataFrame(results)


        #
        # Summary statistics logging
        #
        total_documents = len(result_df)

        documents_with_urls = (
            result_df["ContainsURL"]
            .sum()
        )

        percentage = (
            documents_with_urls / total_documents * 100
            if total_documents > 0
            else 0
        )


        self.logger.info(
            f"Documents analysed: {total_documents}"
        )

        self.logger.info(
            f"Documents containing URLs: "
            f"{documents_with_urls} "
            f"({percentage:.2f}%)"
        )


        #
        # Optional save
        #
        if saver_name:

            saver = DataSaverFactory.get_saver(saver_name)

            if saver is None:
                raise ValueError(
                    f"Data saver '{saver_name}' not found"
                )


            connector = SQLAlchemyConnector(
                **connector_params
            )


            saver.save(
                df=result_df,
                table_name=table_name,
                schema=schema,
                if_exists=if_exists,
                chunk_size=chunk_size,
                connector=connector
            )


        #
        # Visualisations
        #
        for viz in viz_params:

            viz_name = viz["name"]

            viz_config = {
                k: v
                for k, v in viz.items()
                if k not in ["name", "filename"]
            }

            filename = viz.get(
                "filename",
                "chart.png"
            )


            visualisation = (
                self.visualisation_factory
                .get_visualisation(
                    viz_name,
                    **viz_config
                )
            )


            #
            # Create plotting dataframe
            #
            plot_data = (
                result_df["ContainsURL"]
                .map(
                    {
                        True: "Contains URL",
                        False: "No URL"
                    }
                )
            )

            fig, ax = visualisation.plot(
                plot_data
            )


            fig.savefig(
                os.path.join(
                    save_path,
                    filename
                ),
                bbox_inches="tight"
            )

            plt.close(fig)


        return result_df