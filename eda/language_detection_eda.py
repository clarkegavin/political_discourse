from scipy.interpolate import insert

from eda.base import EDAComponent
from visualisations.factory import VisualisationFactory
from data.savers.factory import DataSaverFactory
from data.sqlalchemy_connector import SQLAlchemyConnector
#from ftlangdetect import detect
from ftlangdetect import detect
import pandas as pd
from logs.logger import get_logger
import os
import matplotlib.pyplot as plt


class LanguageDetectionEDA(EDAComponent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.visualisation_factory = VisualisationFactory()
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized LanguageDetectionEDA")

    def _detect_language(self, text: str):
        if not text.strip():
            return None, None

        try:
            result = detect(text)
            return result.get("lang"), result.get("score")

        except Exception as e:
            self.logger.error(f"Error detecting language: {e}")
            return None, None

    def run(self, data: pd.DataFrame, target=None, text_field=None, save_path=None,**kwargs):
        columns = kwargs.get('columns', [])
        detector = kwargs.get('detector', 'fasttext-langdetect')
        confidence_threshold = kwargs.get('confidence_threshold', 0.7)
        saver_name = kwargs.get('saver_name')
        table_name = kwargs.get('table_name')
        schema = kwargs.get('schema')
        if_exists = kwargs.get('if_exists', 'fail')
        chunk_size = kwargs.get('chunk_size', 1000)
        connector_params = kwargs.get('connector_params', {})
        viz_params = kwargs.get('viz_params', [])

        if save_path is None:
            save_path = os.getcwd()

        results = []
        for column in columns:
            if column not in data.columns:
                self.logger.warning(f"Column {column} not found in data.")
                continue
            self.logger.info(f"Data language - Data Length: {len(data)} ")

            for idx, text in data[column].items():
                question_id = data.loc[idx, 'QuestionId'] if 'QuestionId' in data.columns else None
                document_id = data.loc[idx, 'DocumentId'] if 'DocumentId' in data.columns else None
                text = "" if pd.isna(text) else str(text)
                lang, score = self._detect_language(text)

                text_length = len(text)

                results.append({
                    'QuestionID':question_id,
                    'DocumentID':document_id,
                    'RecordID': idx,
                    'ColumnName': column,
                    'Language': lang,
                    'Confidence': score,
                    'AboveThreshold': score is not None and score >= confidence_threshold,
                    'IsIrish': lang == 'ga' if lang else None,
                    'TextLength': text_length,
                    'TextSample': text[:150]
                })

        result_df = pd.DataFrame(results)

        if saver_name:
            saver = DataSaverFactory.get_saver(saver_name)
            if saver is None:
                self.logger.error(f"Data saver '{saver_name}' not found in DataSaverFactory")
                raise ValueError(f"Data saver '{saver_name}' not found in DataSaverFactory")

            try:
                connector = SQLAlchemyConnector(**connector_params)
            except Exception as e:
                self.logger.error(f"Failed to initialize SQLAlchemyConnector: {e}")
                raise

            try:
                saver.save(
                    df=result_df,
                    table_name=table_name,
                    schema=schema,
                    if_exists=if_exists,
                    chunk_size=chunk_size,
                    connector=connector
                )
            except Exception as e:
                self.logger.error(f"Failed to save result DataFrame: {e}")
                raise

        # for viz in viz_params:
        #     viz_instance = VisualisationFactory.get_visualisation(viz['name'], **viz)
        #     fig, ax = viz_instance.plot(result_df)
        #     fig.savefig(viz['filename'])
        #     fig.clf()

        for viz in viz_params:
            viz_name = viz["name"]
            viz_config = {
                k: v for k, v in viz.items()
                if k not in ["name", "filename"]  # Exclude keys that are not relevant for visualisation config
            }
            filename = viz["filename"] if "filename" in viz else "chart.png"
            self.logger.info(f"Preparing visualisation '{viz_name}' with config: {viz_config}")

            visualisation = self.visualisation_factory.get_visualisation(
                viz_name,
                **viz_config
            )

            self.logger.info(f"Creating visualisation '{viz_name}' with config: {viz_config}")
            fig, ax = visualisation.plot(result_df)

            if filename:
                fig.savefig(os.path.join(save_path, filename), bbox_inches="tight")

            plt.close(fig)

        return result_df
