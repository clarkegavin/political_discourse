#visualisations/bar_chart.py
from .base import Visualisation
from logs.logger import get_logger

class BarChart(Visualisation):
    """
    Bar Chart Visualisation.
    """

    def __init__(self, title: str, xlabel=None, ylabel=None, figsize=(10,6), **params):
        super().__init__(title=title, figsize=figsize)
        self.logger = get_logger(self.__class__.__name__)
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.figsize = figsize
        self.params = params  # optional style parameters
        self.logger.info(f"Initialized Bar Chart visualisation with title: {title}, xlabel: {xlabel}, ylabel: {ylabel}, figsize: {figsize}, params: {params}")

    def build(self):
        self.logger.info(f"Built Bar Chart visualisation with params: {self.params}")
        return self


    def plot(self, data, ax=None, title=None, **kwargs):
        """
        Draw a bar chart.
        - For categorical inputs (Series of object/categorical dtype, list-like of strings), compute value counts and plot frequencies.
        - If there are more than `top_n` categories (default 20), only plot the top `top_n` by frequency.
        - Accepts dict-like {label: value} by plotting values (limited to top_n labels by value if >top_n).
        - If `ax` is provided, draw into it; otherwise create a new figure.
        - If `exclude_columns` is provided in params and title matches an excluded column, skip plotting and show a small placeholder.
        Returns (fig, ax) or (None, None) on failure.
        """
        import matplotlib.pyplot as plt
        try:
            import pandas as pd
            import numpy as np
        except Exception:
            pd = None
            np = None

        self.logger.info(f"Creating Bar Chart visualisation with data type: {type(data)}")

        created_fig = None
        if ax is None:
            created_fig, ax = plt.subplots(figsize=self.figsize)
        else:
            created_fig = ax.figure

        # Default top_n for categorical trimming
        top_n = int(self.params.get("top_n", 20))

        # Excluded columns handling
        exclude = self.params.get('exclude_columns') or []
        # normalize exclude to a list of strings
        if isinstance(exclude, (str,)):
            exclude = [exclude]
        try:
            exclude = list(exclude)
        except Exception:
            exclude = []

        # If title matches an excluded column, skip plotting and place a placeholder in the axis
        # Determine effective plot title/name for exclusion checks
        plot_title = title or getattr(data, 'name', None) or self.title

        # Build a robust exclude set with multiple normalisations (raw, lower, stripped, alphanumeric-only)
        def _norms(x):
            s = '' if x is None else str(x)
            s_raw = s
            s_lower = s.strip().lower()
            s_alnum = ''.join(ch.lower() for ch in s if ch.isalnum())
            s_stripped = s.replace('_', '').replace(' ', '').lower()
            return {s_raw, s_lower, s_alnum, s_stripped}

        exclude_variants = set()
        for ex in exclude:
            try:
                exclude_variants.update(_norms(ex))
            except Exception:
                continue

        # build variants for plot title and data column name
        title_variants = set()
        try:
            title_variants.update(_norms(plot_title))
        except Exception:
            pass
        try:
            title_variants.update(_norms(getattr(data, 'name', None)))
        except Exception:
            pass
        title_variants.update(_norms(self.title))

        # If any normalized variant matches, skip plotting
        if exclude_variants.intersection(title_variants):
            self.logger.info(f"Skipping bar chart for excluded column: {plot_title}")
            try:
                ax.set_visible(False)
            except Exception:
                pass
            return created_fig, ax

        try:
            labels = None
            values = None

            # Dict-like input: treat as mapping label->value; if too many categories, take top by value
            if hasattr(data, "items") and not hasattr(data, "values"):
                items = list(data.items())
                # ensure values are numeric-ish for sorting; if not, convert to counts of keys
                try:
                    # sort by value descending
                    items_sorted = sorted(items, key=lambda kv: (kv[1] is None, -float(kv[1]) if kv[1] is not None else 0))
                except Exception:
                    items_sorted = items
                if len(items_sorted) > top_n:
                    items_sorted = items_sorted[:top_n]
                labels, values = zip(*items_sorted) if items_sorted else ([], [])

            # Pandas Series: compute value_counts to get frequencies
            elif pd is not None and isinstance(data, pd.Series):
                try:
                    counts = data.value_counts(dropna=False)
                except TypeError:
                    # unhashable items (lists/dicts) inside cells; convert to string representation then count
                    ser = data.apply(lambda x: ','.join(map(str, x)) if isinstance(x, (list, tuple, set)) else str(x))
                    counts = ser.value_counts(dropna=False)
                if len(counts) > top_n:
                    counts = counts.nlargest(top_n)
                labels = list(counts.index)
                values = list(counts.values)

            # List-like or ndarray: convert to Series and compute counts
            else:
                try:
                    if pd is not None:
                        ser = pd.Series(data)
                        try:
                            counts = ser.value_counts(dropna=False)
                        except TypeError:
                            ser2 = ser.apply(lambda x: ','.join(map(str, x)) if isinstance(x, (list, tuple, set)) else str(x))
                            counts = ser2.value_counts(dropna=False)
                        if len(counts) > top_n:
                            counts = counts.nlargest(top_n)
                        labels = list(counts.index)
                        values = list(counts.values)
                    else:
                        # fallback: iterate and plot raw values (may be numeric)
                        values = list(data)
                        labels = list(range(len(values)))
                except Exception:
                    # final fallback: try to treat data as simple mapping of index->value
                    try:
                        labels = getattr(data, 'index', None)
                        values = getattr(data, 'values', data)
                    except Exception:
                        labels = None
                        values = data

            # Ensure labels are string-friendly when categorical
            if labels is not None:
                labels_plot = [str(l) for l in labels]
                # Truncate long labels for readability if requested via params
                try:
                    max_chars = int(self.params.get("label_max_chars", 10))
                except Exception:
                    max_chars = 10
                if max_chars and max_chars > 0:
                    def _truncate(s):
                        if len(s) <= max_chars:
                            return s
                        # leave room for ellipsis
                        if max_chars > 3:
                            return s[: max_chars - 3] + "..."
                        return s[:max_chars]
                    truncated_labels = [_truncate(s) for s in labels_plot]
                else:
                    truncated_labels = labels_plot

                # Ensure values are numeric (counts). Coerce and fill NaN with 0.
                try:
                    import numpy as _np
                    values_arr = _np.array(list(values), dtype=float)
                    # If values are booleans, convert to int
                    if values_arr.dtype == bool:
                        values_arr = values_arr.astype(int)
                    # Replace NaNs with 0
                    values_arr[_np.isnan(values_arr)] = 0
                except Exception:
                    # fallback: attempt to convert via pandas
                    try:
                        import pandas as _pd
                        values_arr = _pd.to_numeric(list(values), errors='coerce').fillna(0).values
                    except Exception:
                        values_arr = list(values)

                ax.bar(truncated_labels, values_arr, **kwargs)
                # ensure tick positions match labels and set rotated labels if requested
                try:
                    ax.set_xticks(range(len(truncated_labels)))
                    rotation = self.params.get("xticks_rotation")
                    if rotation is not None:
                        ax.set_xticklabels(truncated_labels, rotation=rotation)
                    else:
                        ax.set_xticklabels(truncated_labels)
                except Exception:
                    # fallback: set_xticklabels directly
                    rotation = self.params.get("xticks_rotation")
                    if rotation is not None:
                        ax.set_xticklabels(truncated_labels, rotation=rotation)
                    else:
                        ax.set_xticklabels(truncated_labels)
            else:
                # Fallback numeric conversion for values without labels
                try:
                    import numpy as _np
                    values_arr = _np.array(list(values), dtype=float)
                    values_arr[_np.isnan(values_arr)] = 0
                except Exception:
                    values_arr = list(values)
                ax.bar(list(range(len(values_arr))), values_arr, **kwargs)

            # Apply labels / title (title param overrides instance title)
            ax.set_title(plot_title)
            if self.xlabel:
                ax.set_xlabel(self.xlabel)
            if self.ylabel:
                ax.set_ylabel(self.ylabel)
            else:
                # default y-axis label
                ax.set_ylabel('Count')

            rotation = self.params.get("xticks_rotation")
            if rotation is not None:
                ax.tick_params(axis='x', labelrotation=rotation)

            self.logger.info("Bar Chart visualisation created")
            return created_fig, ax

        except Exception as e:
            self.logger.exception(f"Error plotting bar chart: {e}")
            return None, None
