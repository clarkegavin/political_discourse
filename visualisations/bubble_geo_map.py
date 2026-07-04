import matplotlib.pyplot as plt
import numpy as np
import geopandas as gpd
from matplotlib import patheffects as pe
from .base import Visualisation
from logs.logger import get_logger


class BubbleGeoMapVisualisation(Visualisation):

    def __init__(self, title, filename, name_field=None, figsize=(10, 10)):
        super().__init__(title=title, figsize=figsize)
        self.logger = get_logger(self.__class__.__name__)
        self.title = title
        self.filename = filename
        self.name_field = name_field  # constituency column (from viz_params)
        self.figsize = figsize

    def _prepare_data(self, geo_df, value_field, group_field):
        """
        Ensures:
        - 1 geometry per constituency (dissolve)
        - stable centroid computation
        - clean aggregation alignment
        """
        self.logger.info(f"Preparing data for bubble geo map: value={value_field}, group={group_field}")
        df = geo_df.copy()

        # --- ensure valid GeoDataFrame
        if not isinstance(df, gpd.GeoDataFrame):
            raise ValueError("geo_df must be a GeoDataFrame")

        # --- dissolve ensures ONE geometry per constituency (critical fix)
        #df = df.dissolve(by=group_field, as_index=False)

        # --- recompute centroid AFTER dissolve (correct spatial logic)
        df["centroid"] = df.geometry.representative_point()

        return df

    def _scale_bubbles(self, values):
        """
        Robust visual scaling:
        - log compresses skew
        - min-max normalisation ensures comparability
        """
        self.logger.info("Scaling bubble sizes for visualisation")

        # if it is not numpy
        if not isinstance(values, np.ndarray):
            values = values.astype(float).fillna(0)
        else:
            values = np.nan_to_num(values.astype(float), nan=0.0)

        # log transform reduces dominance of large constituencies
        #values = np.log1p(values)

        # normalise to 5–40 px range (visually stable for maps)
        min_size, max_size = 5, 40

        #vmin, vmax = values.min(), values.max()
        vmin = np.percentile(values, 5)
        vmax = np.percentile(values, 95)
        values_clipped = np.clip(values, vmin, vmax)
        min_size = 100
        max_size = 1200


        #norm = (values - vmin) / (vmax - vmin + 1e-9)
        norm = (values_clipped - vmin) / (vmax - vmin + 1e-9)
        sizes = norm * (max_size - min_size) + min_size
        #sizes = norm * (200-40) + 40  # scale to 40–200 px for visibility
        return sizes

    def plot(self, geo_df, value_field, group_field=None):
        self.logger.info(f"Creating bubble geo map visualisation: {self.title} with value field: {value_field} and group field: {group_field}")
        group_field = group_field or self.name_field

        self.logger.info(
            f"Creating bubble geo map: {self.title} "
            f"(value={value_field}, group={group_field})"
        )

        # -------------------------
        # 1. PREP DATA
        # -------------------------
        df = self._prepare_data(geo_df, value_field, group_field)

        centroids = df["centroid"]
        lons = centroids.x
        lats = centroids.y

        self.logger.info(f"CRS: {df.crs}")
        self.logger.info(f"Centroid sample: {lons.iloc[0]}, {lats.iloc[0]}")

        values = df[value_field].fillna(0)
        sizes = self._scale_bubbles(values)
        self.logger.info(f"Bubble sizes scaled: min={sizes.min()}, max={sizes.max()}")

        # -------------------------
        # 2. FIGURE
        # -------------------------
        fig, ax = plt.subplots(1, 1, figsize=self.figsize)
        self.logger.info(f"Figure created with size: {self.figsize}")
        # -------------------------
        # 3. BASE MAP (cartographic green)
        # -------------------------
        df.plot(
            ax=ax,
            color="#eaf4ea",     # light cartographic green
            edgecolor="#4d4d4d",
            linewidth=0.4,
            #alpha=0.3,
            zorder=1
        )
        self.logger.info("Base map plotted with light green fill and dark edges")
        # -------------------------
        # 4. BUBBLES
        # -------------------------
        ax.scatter(
            lons,
            lats,
            s=sizes,
            color="#5B9BD5",#1F4E79
            alpha=0.75,
            edgecolor="#5B9BD5",
            linewidth=0.3,
            zorder=10
        )
        self.logger.info(f"Plotted {len(sizes)} bubbles on the map with sizes scaled to values in '{value_field}'")
        # -------------------------
        # 5. LABELS (white halo text)
        # -------------------------

        for x, y, label in zip(lons, lats, df[group_field]):
            self.logger.info(f"Adding label '{label}' at coordinates ({x}, {y})")
            txt = ax.text(
                x, y,
                str(label),
                fontsize=6,
                ha="center",
                va="center",
                color="black",
                zorder=4
            )
            self.logger.info(f"Added label '{label}' at ({x}, {y}) with black text and white halo for readability")
            # white halo for readability (key improvement)
            txt.set_path_effects([
                pe.Stroke(linewidth=3, foreground="white"),
                pe.Normal()
            ])
        # -----------------------
        # 5.5 LEGEND (optional)
        # ----------------------
        legend_values = [
            int(values.quantile(0.25)),
            int(values.quantile(0.50)),
            int(values.quantile(0.75)),
            int(values.max())
        ]

        legend_sizes = self._scale_bubbles(np.array(legend_values))

        handles = [
            ax.scatter(
                [],
                [],
                s=size,
                color="#5B9BD5",
                edgecolor="#1F4E79",
                alpha=0.75
            )
            for size in legend_sizes
        ]

        ax.legend(
            handles,
            [f"{v:,}" for v in legend_values],
            title="Questions",
            scatterpoints=1,
            loc="lower right",
            frameon=True,
            fontsize=9,
            title_fontsize=10,
            borderpad=0.8,
            labelspacing=1.5
        )

        # -------------------------
        # 6. STYLING
        # -------------------------
        self.logger.info("Finalising plot styling: title, axis off")
        #ax.set_title(self.title, fontsize=14)
        ax.set_axis_off()
        ax.autoscale(enable=False)
        return fig, ax