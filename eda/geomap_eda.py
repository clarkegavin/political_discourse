#eda/geomap_eda.py
import geopandas as gpd
import pandas as pd
import re
import os
from visualisations.factory import VisualisationFactory
from logs.logger import get_logger
from .base import EDAComponent

class GeoMapEDA(EDAComponent):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized GeoMapEDA")
        params = kwargs.get('params', {})
        self.shape_file = None
        self.constituency = None
        self.remove_trailing_numbers = None
        self.group_by_field = None
        self.value_field = None
        self.aggregation_method = None
        self.viz_params = None

    def _normalize_constituency(self, value):
        if self.remove_trailing_numbers:
            return re.sub(r"\s*\(\d+\)$", "", value)
        return value

    def run(self, data, save_path=None, **kwargs):

        params = kwargs
        self.shape_file = params.get('shape_file')
        self.logger.info(f"Shape file path: {self.shape_file}")
        self.constituency = params.get('constituency')
        self.remove_trailing_numbers = params.get('remove_trailing_numbers', False)
        self.group_by_field = params.get('group_by_field')
        self.value_field = params.get('value_field')
        self.aggregation_method = params.get('aggregation_method', 'count')
        self.viz_params = params.get('viz_params', [])
        self.name_field = params.get('name_field', None) # for interative maps - hover text
        if save_path is None:
            save_path = os.getcwd()
            self.logger.info(f"No save_path provided. Using current working directory: {save_path}")
        self.logger.info(f"Save path for geomap visualisations: {save_path}")
        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("GeoMapEDA requires a pandas DataFrame")

        # Load GeoJSON file
        self.logger.info(f"Loading GeoJSON file from {self.shape_file}")
        self.logger.info(f"Current working directory: {os.getcwd()}")
        geo_df = gpd.read_file(self.shape_file)
        # Normalize constituency names in GeoJSON
        geo_df[self.constituency] = geo_df[self.constituency].apply(self._normalize_constituency)
        # Normalize constituency names in DataFrame
        data[self.group_by_field] = data[self.group_by_field].apply(self._normalize_constituency)

        geo_names = set(geo_df[self.constituency].dropna().unique())
        data_names = set(data[self.group_by_field].dropna().unique())

        missing_from_data = sorted(geo_names - data_names)
        missing_from_geo = sorted(data_names - geo_names)

        self.logger.info(f"GeoJSON constituencies: {len(geo_names)}")
        self.logger.info(f"Data constituencies: {len(data_names)}")

        self.logger.warning(f"In GeoJSON but not data ({len(missing_from_data)}): {missing_from_data}")
        self.logger.warning(f"In data but not GeoJSON ({len(missing_from_geo)}): {missing_from_geo}")


        self.logger.info(f"Data columns before merging: {data.columns}")
        self.logger.info(f"GeoDataFrame columns before merging: {geo_df.columns}")

        # Aggregate data
        aggregated_df = data.groupby(self.group_by_field).agg({self.value_field: self.aggregation_method}).reset_index()


        # Merge with GeoJSON

        geo_df = geo_df.dissolve(by=self.constituency, as_index=False) # dissolve to ensure unique constituency geometries
        geo_df = geo_df.merge(aggregated_df, left_on=self.constituency, right_on=self.group_by_field, how="left")
        self.logger.info(f"GeoDataFrame value counts after merging: {geo_df[self.constituency].value_counts().head(10)}")
        self.logger.info(f"GeoDataFrame row count after merging: {len(geo_df)}")
        self.logger.info(f"GeoDataFrame data after merging: {geo_df.head(10)}")


        unmatched = geo_df[geo_df[self.value_field].isna()]

        self.logger.info(f"Number of unmatched constituencies: {len(unmatched)}")

        if not unmatched.empty:
            self.logger.warning(
                f"Unmatched constituency names: "
                f"{sorted(unmatched[self.constituency].tolist())}"
            )


        # Visualize
        for viz_param in self.viz_params:
            viz_params = dict(viz_param)
            #viz_params.setdefault('output_dir', save_path)

            visualisation = VisualisationFactory.get_visualisation(viz_params.pop('name'), **viz_params)
            if visualisation is None:
                raise KeyError(f"Visualisation '{viz_param['name']}' not registered in VisualisationFactory")

            try:
                self.logger.info(f"Creating geomap visualisation")
                fig, ax = visualisation.plot(geo_df, value_field=self.value_field, group_field = self.group_by_field)
                self.logger.info(f"Created geomap visualisation: {viz_params.get('name')}")
                filename = viz_params.get('filename', 'geomap_visualisation.png')
                outpath = os.path.join(save_path, filename)
                self.logger.info(f"Saving geomap visualisation to {outpath}")
                visualisation.save(fig, outpath)
                self.logger.info(f"Saved geomap visualisation to {outpath}")
            except Exception as e:
                self.logger.warning(f"Failed to save geomap visualisation: {e}")
