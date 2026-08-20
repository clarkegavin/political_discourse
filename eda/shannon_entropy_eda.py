import numpy as np
import pandas as pd
from .base import EDAComponent
from logs.logger import get_logger
from data.savers.factory import DataSaverFactory
from data.sqlalchemy_connector import SQLAlchemyConnector
from visualisations.factory import VisualisationFactory
import os
import matplotlib.pyplot as plt

class ShannonEntropyEDA(EDAComponent):
    def __init__(self, group_by_field=None, category_field=None, normalise=True, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger(self.__class__.__name__)
        self.group_by_field = group_by_field
        self.category_field = category_field
        self.normalise = normalise
        self.visualisation_factory = VisualisationFactory()
        self.logger.info(f"Initialized ShannonEntropyEDA with group_by_field={group_by_field}, category_field={category_field}, normalise={normalise}")

    def run(self, data, target=None, text_field=None, save_path=None,**kwargs):
        """
        Compute Shannon entropy for grouped categorical data and optionally save and visualise the results.

        Parameters:
        - df: pandas DataFrame
        - kwargs: may contain overrides for group_by_field, category_field, normalise, and optional saver/visualisation params
        """
        # Resolve configuration: instance values take precedence; kwargs override when provided
        group_by_field = kwargs.get('group_by_field', self.group_by_field)
        category_field = kwargs.get('category_field', self.category_field)
        normalise = kwargs.get('normalise', self.normalise)

        if save_path is None:
            save_path = os.getcwd()

        # Saver params (optional)
        saver_name = kwargs.get('saver_name')
        table_name = kwargs.get('table_name')
        if_exists = kwargs.get('if_exists', 'replace')
        chunk_size = kwargs.get('chunk_size', 1000)
        schema = kwargs.get('schema')
        connector_params = kwargs.get('connector_params', {}) or {}

        # Visualisation params
        viz_params = kwargs.get('viz_params', [])

        # Validate data
        if not isinstance(data, pd.DataFrame):
            self.logger.error("`df` must be a pandas DataFrame")
            raise ValueError("`df` must be a pandas DataFrame")

        self.logger.info(f"ShannonEntropyEDA received DataFrame with {len(data)} records and columns: {list(data.columns)}")

        # Compute Shannon entropy
        grouped = data.groupby([group_by_field, category_field]).size().reset_index(name='Count')
        total_counts = grouped.groupby(group_by_field)['Count'].sum().reset_index(name='TotalCount')
        num_categories = grouped.groupby(group_by_field).size().reset_index(name='NumCategories')

        merged = pd.merge(grouped, total_counts, on=group_by_field)
        merged = pd.merge(merged, num_categories, on=group_by_field)

        merged['p_i'] = merged['Count'] / merged['TotalCount']
        merged['ShannonEntropy'] = -merged['p_i'] * np.log(merged['p_i'])
        entropy = merged.groupby(group_by_field).agg({
            'ShannonEntropy': 'sum',
            'TotalCount': 'first',
            'NumCategories': 'first'
        }).reset_index()

        if normalise:
            entropy['NormalisedEntropy'] = entropy.apply(
                lambda row: row['ShannonEntropy'] / np.log(row['NumCategories']) if row['NumCategories'] > 1 else 0,
                axis=1
            )
        else:
            entropy['NormalisedEntropy'] = entropy['ShannonEntropy']

        self.logger.info("Shannon entropy calculation complete")


        # Save results
        # if saver_name:
        #     saver = DataSaverFactory.get_saver(saver_name, **connector_params)
        #     saver.save(entropy, table_name=table_name, schema=schema, if_exists=if_exists, chunk_size=chunk_size)
        #     self.logger.info(f"Saved ShannonEntropyEDA results to {table_name}")

        # Optionally save the term-frequency DataFrame using the DataSaverFactory (reuse DataSaverPipeline behaviour)
        if table_name:
            # Use provided saver_name or default to 'sql_server'
            saver_name = saver_name or 'sql_server'
            self.logger.info(f"Saving term-frequency DataFrame to table '{table_name}' using saver '{saver_name}'")
            saver = DataSaverFactory.get_saver(saver_name)
            if saver is None:
                self.logger.error(f"Data saver '{saver_name}' not found in DataSaverFactory")
                raise ValueError(f"Data saver '{saver_name}' not found in DataSaverFactory")

            # Instantiate connector
            try:
                connector = SQLAlchemyConnector(
                    **connector_params) if connector_params is not None else SQLAlchemyConnector()
            except Exception as e:
                self.logger.error(f"Failed to initialize SQLAlchemyConnector: {e}")
                raise

            try:
                saver.save(
                    df=entropy,
                    table_name=table_name,
                    connector=connector,
                    if_exists=if_exists,
                    chunk_size=chunk_size,
                    schema=schema,
                )
            except Exception as e:
                self.logger.error(f"Failed to save term-frequency DataFrame: {e}")
                raise

        #filename = kwargs.get('filename') or 'entropy_histogram.png'

        # Visualise results
        for viz in viz_params:
            viz_name = viz["name"]
            viz_config = {
                k: v for k, v in viz.items()
                if k not in ["name", "column", "filename"]  # Exclude keys that are not relevant for visualisation config
            }
            filename =viz["filename"] if "filename" in viz else "entropy_histogram.png"
            self.logger.info(f"Preparing visualisation '{viz_name}' with config: {viz_config}")

            visualisation = self.visualisation_factory.get_visualisation(
                viz_name,
                **viz_config
            )

            plot_data = entropy
            if "column" in viz:
                self.logger.info(f"Using specified column for visualisation: {viz['column']}")
                plot_data = entropy[viz["column"]]


            self.logger.info(f"Creating visualisation '{viz_name}' with config: {viz_config}")
            fig, ax = visualisation.plot(plot_data, **viz_config)


            if filename:
                fig.savefig(os.path.join(save_path, filename), bbox_inches="tight")

            plt.close(fig)

        return entropy

