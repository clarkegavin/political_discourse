# visualisations/intertopic_distance.py
from .base import Visualisation
from logs.logger import get_logger
import os
from typing import Optional

class IntertopicDistance(Visualisation):
    """
    Visualisation that saves BERTopic's intertopic distance map (plotly) as an HTML file.

    Constructor params (init):
      - title: str (inherited)
      - output_dir: directory to save HTML
      - filename: filename for saved HTML (default: intertopic_distance.html)
      - figsize: kept for compatibility but unused for plotly
      - any extra params are stored in self.params

    plot signature matches existing patterns: plot(data, model=None, save_path=None, **plot_kwargs)
    Returns the saved HTML path (string) or None on failure.
    """

    def __init__(self, name: str = "intertopic_distance", title: str = "Intertopic Distance Map",
                 output_dir: str = ".", filename: str = "intertopic_distance.html", figsize=(10, 6), **params):
        super().__init__(title=title, figsize=figsize)
        self.logger = get_logger(self.__class__.__name__)
        self.name = name
        self.output_dir = output_dir
        self.filename = filename
        self.params = params or {}
        self.logger.info(f"Initialized IntertopicDistance visualisation with output_dir={output_dir}, filename={filename}, params={params}")

    def plot(self, data, model=None, save_path: Optional[str] = None, filename: Optional[str] = None, **plot_kwargs):
        """
        Create and save BERTopic intertopic distance map.

        - data: dataframe (ignored by BERTopic visualize_topics)
        - model: expected to be a BERTopicModel wrapper or the underlying BERTopic instance
        - save_path: optional directory to save; falls back to self.output_dir
        - filename: optional filename to use; falls back to self.filename
        - plot_kwargs: passed to visualize_topics() if supported

        Returns path to saved HTML on success, otherwise None.
        """
        # Determine BERTopic instance: unwrap wrapper if necessary
        bertopic_model = None
        if model is None:
            self.logger.warning("No model provided to IntertopicDistance visualisation")
            return None

        # Unwrap common wrapper pattern where ModelFactory returns an object with `.model`
        bertopic_model = getattr(model, "model", model)

        if not hasattr(bertopic_model, "visualize_topics"):
            self.logger.warning("Provided model does not expose `visualize_topics()`; cannot render intertopic distance map")
            return None

        try:
            # call visualize_topics with any plot-specific kwargs
            fig = bertopic_model.visualize_topics(**(plot_kwargs or {}))
        except Exception as e:
            self.logger.exception(f"Failed to generate intertopic distance map: {e}")
            return None

        # Determine where to save
        out_dir = save_path or self.output_dir or "."
        fname = filename or self.filename or "intertopic_distance.html"
        try:
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, fname)
            # Plotly figure has write_html
            fig.write_html(out_path)
            self.logger.info(f"Saved intertopic distance map to {out_path}")
            return out_path
        except Exception as e:
            self.logger.exception(f"Could not save intertopic distance map to {out_dir}/{fname}: {e}")
            return None

