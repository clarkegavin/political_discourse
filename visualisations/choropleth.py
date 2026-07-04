import matplotlib.pyplot as plt
import geopandas as gpd
from .base import Visualisation

class ChoroplethVisualisation(Visualisation):
    def __init__(self, title, colour_map, missing_colour, filename):
        super().__init__(title=title, figsize=(10, 10))
        self.title = title
        self.colour_map = colour_map
        self.missing_colour = missing_colour
        self.filename = filename

    def plot(self, geo_df, value_field, group_field=None):
        # Plot choropleth
        fig, ax = plt.subplots(1, 1, figsize=(10, 10))
        geo_df.plot(
            column=value_field,
            cmap=self.colour_map,
            missing_kwds={"color": self.missing_colour},
            legend=True,
            ax=ax
        )
        ax.set_title(self.title)

        return fig, ax
        #plt.savefig(self.filename)
        #plt.close()
