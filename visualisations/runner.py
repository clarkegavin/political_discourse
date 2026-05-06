# visualisations/runner.py
from typing import Dict, Any, List
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory

logger = get_logger("VisualisationRunner")


class VisualisationRunner:
    """Responsible for creating visualisation instances (with init params) and calling plot() with plot params.

    Simple contract:
      viz_cfg: either a dict with 'name', optional 'init' dict, and optional 'plot' dict;
               or a dict with keys where all except 'name' are treated as plot params and init is empty.

    This runner does NOT perform MLflow logging; ExperimentRunner will log returned artifact paths.
    """

    def __init__(self):
        self.logger = logger

    def render(self, result: Dict[str, Any], viz_cfg: Dict[str, Any], model=None) -> List[str]:
        """Render a visualization and return a list of saved artifact paths produced by the viz.

        Expect visualisation classes to accept instantiation params via constructor and to expose a plot(df, model=None, **plot_kwargs)
        that returns either a path or list of paths or a Matplotlib Figure (in which case the caller is responsible for saving).
        """
        name = viz_cfg.get("name")
        init_params = viz_cfg.get("init", {})
        plot_params = viz_cfg.get("plot", {})

        # Backwards compatibility: if 'init' not present and viz_cfg has other keys, treat them as plot params
        if not init_params:
            plot_params = {k: v for k, v in viz_cfg.items() if k != "name"}

        self.logger.info(f"Instantiating visualisation '{name}' with init_params: {init_params}")
        viz = VisualisationFactory.get_visualisation(name, **init_params)
        if not viz:
            self.logger.warning(f"VisualisationFactory returned None for '{name}'")
            return []

        try:
            self.logger.info(f"Rendering visualisation '{name}'")
            # Standardized call
            out = viz.plot(result.get("df"), model=model, **plot_params)

            # Normalize outputs into list of artifact paths
            if out is None:
                return []
            if isinstance(out, list):
                return out
            # If a string path
            if isinstance(out, str):
                return [out]
            # If a figure-like object, we cannot auto-save (caller should handle); return empty
            return []

        except Exception as e:
            self.logger.error(f"Visualisation '{name}' failed: {e}")
            return []

