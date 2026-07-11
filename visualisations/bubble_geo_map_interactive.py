import plotly.graph_objects as go
from logs.logger import get_logger
from .base import Visualisation
import numpy as np


class InteractiveBubbleGeoMap(Visualisation):

    def __init__(self, title=None, filename='interactive_bubble_geo_map', name_field = None):
        self.logger = get_logger(self.__class__.__name__)
        self.title = title
        self.filename = filename
        self.name_field = name_field

    def plot(self, geo_df, value_field, group_field=None):
        self.logger.info(f"Creating interactive bubble geo map: {self.title}, with value field: {value_field} and name field: {self.name_field}")
        # --- ensure centroid layer
        points = geo_df.copy()
        points["centroid"] = points.geometry.centroid

        lats = points.centroid.y
        lons = points.centroid.x

        #values = points[value_field]
        #values = np.sqrt(points[value_field].fillna(0)) * 5

        raw = points[value_field].fillna(0).astype(float)

        # normalize to 5–30 px range
        min_size = 5
        max_size = 30

        values = (raw - raw.min()) / (raw.max() - raw.min() + 1e-9)
        values = values * (max_size - min_size) + min_size


        self.logger.info(f"interactive bubble geo map data prepared: {points.columns.tolist()}")
        fig = go.Figure()
        self.logger.info(f"Adding choropleth layer for {self.title}")
        # --- Bubble layer
        fig.add_trace(go.Scattergeo(
            lon=lons,
            lat=lats,
            text=points[self.name_field],
            marker=dict(
                size=values,
                color=values,
                colorscale="Blues",
                showscale=True,
                opacity=0.7
            ),
            mode="markers"
        ))

        self.logger.info(f"Configuring layout for {self.title}")
        fig.update_layout(
            title=self.title,
            height=800,
            margin=dict(l=0, r=0, t=50, b=0),
            geo=dict(
                scope="europe",
                projection_type="mercator",
                showland=True,
                landcolor="rgb(245,245,245)",
                center = dict(lat=53.4, lon=-8.0),
                lataxis = dict(range=[51, 56]),
                lonaxis = dict(range=[-11, -5])
            )
        )
        self.logger.info(f"Interactive bubble geo map created: {self.title}")
        return fig, None