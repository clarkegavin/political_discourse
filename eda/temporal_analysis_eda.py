#eda/temporal_analysis_eda.py
import pandas as pd
import numpy as np
from .base import EDAComponent
from logs.logger import get_logger
from data.savers.factory import DataSaverFactory
from visualisations.factory import VisualisationFactory
from data.sqlalchemy_connector import SQLAlchemyConnector
import os
import matplotlib.pyplot as plt

class TemporalAnalysisEDA(EDAComponent):
    def __init__(self, date_field=None, aggregate_by='month', **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger(self.__class__.__name__)
        self.date_field = date_field
        self.aggregate_by = aggregate_by
        self.visualisation_factory = VisualisationFactory()
        self.logger.info(f"Initialized TemporalAnalysisEDA with date_field={date_field}, aggregate_by={aggregate_by}")

    def run(self, data, target=None, text_field=None, save_path=None,**kwargs):
        date_field = kwargs.get('date_field', self.date_field)
        aggregate_by = kwargs.get('aggregate_by', self.aggregate_by)

        if save_path is None:
            save_path = os.getcwd()

        if date_field not in data.columns:
            raise ValueError(f"Date field '{date_field}' not found in data columns.")

        data[date_field] = pd.to_datetime(data[date_field], errors='coerce')
        frequency_map = {
            "day": "D",
            "daily": "D",
            "week": "W",
            "weekly": "W",
            "month": "M",
            "monthly": "M",
            "quarter": "Q",
            "quarterly": "Q",
            "year": "Y",
            "yearly": "Y"
        }

        aggregate_by = frequency_map.get(
            aggregate_by.lower(),
            aggregate_by
        )

        if aggregate_by not in frequency_map.values():
            raise ValueError(
                f"Unsupported aggregation frequency '{aggregate_by}'. "
                f"Use one of: {list(frequency_map.keys())}"
            )

        data['period'] = data[date_field].dt.to_period(aggregate_by).dt.to_timestamp()

        aggregated_data = data.groupby('period').size().reset_index(name='count')

        # include periods where data was missing as zero so as to correctly calculate mean
        period_range = pd.period_range(
            start=aggregated_data['period'].min(),
            end=aggregated_data['period'].max(),
            freq=aggregate_by
        )

        aggregated_data = (
            aggregated_data
            .set_index('period')
            .reindex(period_range.to_timestamp(), fill_value=0)
            .rename_axis('period')
            .reset_index()
        )

        saver_name = kwargs.get('saver_name')
        table_name = kwargs.get('table_name')
        schema = kwargs.get('schema')
        if_exists = kwargs.get('if_exists', 'replace')
        chunk_size = kwargs.get('chunk_size', 1000)
        connector_params = kwargs.get('connector_params', {}) or {}

        if saver_name and table_name:
            saver = DataSaverFactory.get_saver(saver_name)
            if saver is None:
                self.logger.error(f"Data saver '{saver_name}' not found in DataSaverFactory")
                raise ValueError(f"Data saver '{saver_name}' not found in DataSaverFactory")
            #saver = DataSaverFactory.get_saver(saver_name, table_name=table_name, schema=schema, if_exists=if_exists, chunk_size=chunk_size)
            # saver.save(aggregated_data)

            try:
                connector = SQLAlchemyConnector(
                    **connector_params) if connector_params is not None else SQLAlchemyConnector()
            except Exception as e:
                self.logger.error(f"Failed to initialize SQLAlchemyConnector: {e}")
                raise

            try:
                saver.save(
                    df=aggregated_data,
                    table_name=table_name,
                    connector=connector,
                    if_exists=if_exists,
                    chunk_size=chunk_size,
                    schema=schema,
                )
            except Exception as e:
                self.logger.error(f"Failed to save term-frequency DataFrame: {e}")
                raise

        viz_params = kwargs.get('viz_params', [])
        # for viz_param in viz_params:
        #     visualisation = self.visualisation_factory.get_visualisation(viz_param['name'], **viz_param)
        #     visualisation.plot(aggregated_data)
        #     visualisation.save_and_close()
        for viz in viz_params:
            viz_name = viz["name"]
            viz_config = {
                k: v for k, v in viz.items()
                if k not in ["name", "filename"]  # Exclude keys that are not relevant for visualisation config
            }
            filename =viz["filename"] if "filename" in viz else "chart.png"
            self.logger.info(f"Preparing visualisation '{viz_name}' with config: {viz_config}")

            visualisation = self.visualisation_factory.get_visualisation(
                viz_name,
                **viz_config
            )

            self.logger.info(f"Creating visualisation '{viz_name}' with config: {viz_config}")
            fig, ax = visualisation.plot(aggregated_data)

            if filename:
                fig.savefig(os.path.join(save_path, filename), bbox_inches="tight")

            plt.close(fig)

        return aggregated_data
