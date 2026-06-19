# visualisations/sankey.py
from .base import Visualisation
from logs.logger import get_logger
import plotly.graph_objects as go
import os
from typing import Optional


class Sankey(Visualisation):
    """
    Sankey visualisation using Plotly. Expects flow_data in format:
    {
      'nodes': [labels...],
      'links': {'source': [...], 'target': [...], 'value': [...]}
    }

    Constructor params: title, output_dir, filename
    plot signature: plot(data=flow_data, save_path=None, filename=None, **kwargs)
    """

    def __init__(self, title: str = "Sankey Flow", output_dir: str = ".", filename: str = "sankey.html", figsize=(10,6), **kwargs):
        super().__init__(title=title, figsize=figsize)
        self.logger = get_logger(self.__class__.__name__)
        self.output_dir = output_dir
        self.filename = filename
        self.params = kwargs or {}
        self.logger.info(f"Initialized Sankey visualisation with output_dir={output_dir}, filename={filename}")

    def plot(self, data, save_path: Optional[str] = None, filename: Optional[str] = None, **plot_kwargs):
        if data is None or not isinstance(data, dict):
            self.logger.error("Sankey visualisation requires flow data dictionary")
            return None

        nodes = data.get('nodes', [])
        links = data.get('links', {})
        sources = links.get('source', [])
        targets = links.get('target', [])
        values = links.get('value', [])

        if not nodes or not sources:
            self.logger.warning("Empty sankey data; nothing to plot")
            return None

        # ensure deterministic order - nodes are already ordered
        labels = list(nodes)

        # create sankey figure
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                label=labels,
            ),
            link=dict(
                source=sources,
                target=targets,
                value=values
            )
        )])

        fig.update_layout(title_text=plot_kwargs.get('title', self.title), font_size=10)

        out_dir = save_path or self.output_dir or "."
        fname = filename or self.filename or "sankey.html"
        try:
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, fname)
            # write as html using kaleido (plotly supports write_html without kaleido)
            fig.write_html(out_path)
            self.logger.info(f"Saved sankey visualisation to {out_path}")
            return out_path
        except Exception as e:
            self.logger.exception(f"Failed to save sankey visualisation to {out_dir}/{fname}: {e}")
            return None

