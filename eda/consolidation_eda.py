# eda/consolidation_eda.py
"""
EDA component to visualise consolidated vs non-consolidated groups for a specified column.
A group is considered "consolidated" when the count of records for that group > 1.
By default the component counts the number of groups (unique values) in each bucket
but it can also compute the total number of records belonging to consolidated vs
non-consolidated groups by setting metric='records'.

Run signature is compatible with other EDA components:
    run(data, target=None, text_field=None, save_path=None, **kwargs)

Supported kwargs:
- filename: name of file to save (default: 'consolidation_bar.png')
- metric: 'groups' (default) or 'records'
- date_column: optional column name to filter by date
- date_from/date_to: ISO date strings to filter the date_column inclusive
- query: optional pandas query string to filter the DataFrame before analysis
- title/xlabel/ylabel/figsize/xticks_rotation: passed to visualiser

Returns: dict with keys: 'filepath', 'metric', 'groups_consolidated', 'groups_non_consolidated',
         'records_consolidated', 'records_non_consolidated'
"""
from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
import os
import pandas as pd


class ConsolidationEDA(EDAComponent):
    def __init__(self):
        self.logger = get_logger("ConsolidationEDA")
        self.logger.info("Initialized ConsolidationEDA component")

    def _apply_filters(self, data: pd.DataFrame, kwargs: dict) -> pd.DataFrame:
        df = data
        # Apply pandas query if provided
        query = kwargs.get("query")
        if query and isinstance(query, str):
            try:
                df = df.query(query)
            except Exception as e:
                self.logger.warning(f"Failed to apply query '{query}': {e}")
        # Apply date filtering if requested
        date_col = kwargs.get("date_column")
        date_from = kwargs.get("date_from")
        date_to = kwargs.get("date_to")
        if date_col and (date_from or date_to):
            if date_col not in df.columns:
                self.logger.warning(f"date_column '{date_col}' not found in DataFrame; skipping date filter")
            else:
                try:
                    ser = pd.to_datetime(df[date_col], errors='coerce')
                    if date_from:
                        start = pd.to_datetime(date_from)
                        df = df[ser >= start]
                    if date_to:
                        end = pd.to_datetime(date_to)
                        df = df[ser <= end]
                except Exception as e:
                    self.logger.warning(f"Failed to apply date filter on '{date_col}': {e}")
        return df

    def run(self, data, group_by=None, text_field=None, save_path=None, **kwargs):
        """
        data: pandas DataFrame
        target: column name to group by (required)
        """
        target = group_by
        self.logger.info(f"Running ConsolidationEDA on target: {target}")

        if save_path is None:
            save_path = os.getcwd()

        if not isinstance(data, pd.DataFrame):
            self.logger.error("`data` must be a pandas DataFrame")
            raise ValueError("`data` must be a pandas DataFrame")

        if not target:
            self.logger.error("`target` (grouping column) must be provided")
            raise ValueError("`target` (grouping column) must be provided")

        if target not in data.columns:
            self.logger.error(f"Target column '{target}' not found in data")
            raise ValueError(f"Target column '{target}' not found in data")

        # Apply optional filters (date range, query)
        df = self._apply_filters(data, kwargs)

        # Compute counts per group
        try:
            grp = df[target].fillna("__NULL__")
            counts = grp.value_counts(dropna=False)
        except Exception as e:
            self.logger.exception(f"Failed to compute group counts for '{target}': {e}")
            raise

        # groups metric: number of unique groups where count>1 vs ==1
        # cast boolean Series to int before summing to avoid static analysis warnings
        groups_consolidated = int((counts > 1).astype(int).sum())
        groups_non_consolidated = int((counts == 1).astype(int).sum())

        # records metric: total number of records that belong to consolidated groups vs non-consolidated groups
        try:
            records_consolidated = int(counts[counts > 1].sum())
            records_non_consolidated = int(counts[counts == 1].sum())
        except Exception:
            records_consolidated = 0
            records_non_consolidated = 0

        metric = kwargs.get("metric", "groups")
        if metric not in ("groups", "records"):
            self.logger.warning(f"Unknown metric '{metric}', falling back to 'groups'")
            metric = "groups"

        if metric == "groups":
            left = groups_consolidated
            right = groups_non_consolidated
            ylabel = kwargs.get("ylabel") or "Number of groups"
        else:
            left = records_consolidated
            right = records_non_consolidated
            ylabel = kwargs.get("ylabel") or "Number of records"

        #data_for_plot = {"Consolidated": left, "Non-consolidated": right}
        data_for_plot = pd.Series(
            ["Consolidated"] * left + ["Non-consolidated"] * right,
            name="Consolidation"
        )
        # Create visualiser - extract visualization-specific config so we don't
        # pass unrelated kwargs (which could cause duplicate keyword errors).
        # Accept 'group_by' as an alias for 'target'.
        group_col = target or kwargs.pop("group_by", None)

        # viz_params may be provided as a list of dicts (pipeline convention) or a single dict
        viz_params = kwargs.pop("viz_params", None) or kwargs.pop("viz", None)

        viz_name = "bar_chart"
        viz_kwargs = {}
        filename = None

        if viz_params:
            # normalize to a single dict
            if isinstance(viz_params, list) and len(viz_params) > 0:
                cfg = viz_params[0]
            elif isinstance(viz_params, dict):
                cfg = viz_params
            else:
                cfg = None

            if isinstance(cfg, dict):
                # pipeline may use short name 'bar' -> map to registered name
                name_map = {"bar": "bar_chart", "bar_chart": "bar_chart"}
                raw_name = cfg.get("name")
                if raw_name:
                    viz_name = name_map.get(raw_name, raw_name)

                # filename can be provided inside viz config
                filename = cfg.get("filename")

                # remove reserved keys and treat remaining keys as visualiser kwargs
                for k, v in cfg.items():
                    if k in ("name", "filename"):
                        continue
                    viz_kwargs[k] = v

            # when viz_params is present we intentionally DO NOT fall back to top-level
            # title/xlabel/ylabel kwargs to avoid duplicates. The viz config fully
            # controls the visualiser options. If no viz_params were provided below
            # we'll use top-level kwargs.

        # If no viz_params provided, use the top-level kwargs as the visualiser config
        if not viz_kwargs:
            viz_kwargs.setdefault("title", kwargs.pop("title", f"Consolidated vs Non-consolidated ({group_col or target})"))
            viz_kwargs.setdefault("xlabel", kwargs.pop("xlabel", group_col or target))
            viz_kwargs.setdefault("ylabel", kwargs.pop("ylabel", ylabel))
            viz_kwargs.setdefault("figsizefigsize", kwargs.pop("figsize", (8, 5)))
            viz_kwargs.setdefault("xticks_rotation", kwargs.pop("xticks_rotation", 0))
            #viz_kwargs.setdefault("color", kwargs.pop("color", ["#4c72b0", "#55a868"]))
            # allow filename from top-level only when we didn't get it from cfg
            if not filename:
                filename = kwargs.pop("filename", "consolidation_bar.png")
        else:
            # viz_kwargs came from cfg; ensure there's a filename
            if not filename:
                filename = kwargs.pop("filename", "consolidation_bar.png")

        filepath = os.path.join(save_path, filename)

        viz = VisualisationFactory.get_visualisation(viz_name, **viz_kwargs)

        if viz is None:
            self.logger.error("Bar chart visualiser not available")
            raise RuntimeError("Bar chart visualiser not available")

        fig, ax = viz.plot(data=data_for_plot)
        try:
            viz.save(fig, filepath)
            self.logger.info(f"Saved consolidation figure to {filepath}")
        except Exception as e:
            self.logger.error(f"Failed to save consolidation figure: {e}")
            raise

        result = {
            "filepath": filepath,
            "metric": metric,
            "groups_consolidated": groups_consolidated,
            "groups_non_consolidated": groups_non_consolidated,
            "records_consolidated": records_consolidated,
            "records_non_consolidated": records_non_consolidated,
        }

        return result
