# eda/tokenize_text_eda.py
from .base import EDAComponent
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
import os
import pandas as pd
import re
import math
import matplotlib.pyplot as plt
from transformers import AutoTokenizer

def simple_tokenise(text):
    return re.findall(r"\b\w+\b", str(text))


def huggingface_tokenise(text, tokenizer):
    return tokenizer(
        str(text),
        truncation=False,
        add_special_tokens=True
    )["input_ids"]


class TokenizeTextEDA(EDAComponent):
    """
    Tokenize specified text columns and compute token-length statistics.

    Params (via kwargs):
    - columns: list of column names to tokenize
    - output_column: optional name to store token lists (not persisted here)
    - ncols: layout columns for subplots (defaults: for bar_chart subplots use ncols, for boxplot per-column layout uses ncols)
    - viz_params: visualisation specification(s) (list or dict)
    - filename: base filename if not provided inside viz_params
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.logger.info("Initialized TokenizeTextEDA")

    def _compute_token_lengths(
            self,
            df: pd.DataFrame,
            col: str,
            tokenizer_type="lightweight",
            tokenizer=None):

        ser = df[col].fillna("").astype(str)

        if tokenizer_type == "lightweight":
            return ser.apply(
                lambda x: len(simple_tokenise(x))
            )

        elif tokenizer_type == "huggingface":
            return ser.apply(
                lambda x: len(huggingface_tokenise(x, tokenizer))
            )

        else:
            raise ValueError(
                f"Unsupported tokenizer type {tokenizer_type}"
            )

    def run(self, data, target=None, text_field=None, save_path=None, viz_params=None, **kwargs):

        if save_path is None:
            save_path = os.getcwd()

        if data is None or not isinstance(data, pd.DataFrame):
            raise ValueError("TokenizeTextEDA requires a pandas DataFrame")

        cols = kwargs.get("columns") or []

        if isinstance(cols, str):
            cols = [cols]

        cols = [
            c for c in cols
            if c in data.columns
        ]

        if not cols:
            self.logger.error(
                "No columns specified or none found in DataFrame for TokenizeTextEDA"
            )
            raise ValueError(
                "No columns specified or none found in DataFrame for TokenizeTextEDA"
            )

        os.makedirs(save_path, exist_ok=True)

        #
        # ---------------------------------------------------------
        # 1. Lightweight token statistics (EDA)
        # ---------------------------------------------------------
        #

        token_stats = {}
        token_series = {}

        for col in cols:
            tl = self._compute_token_lengths(
                data,
                col,
                tokenizer_type="lightweight"
            )

            token_series[col] = tl

            token_stats[col] = {
                "min_tokens": int(tl.min()),
                "max_tokens": int(tl.max()),
                "avg_tokens": float(tl.mean()),
                "median_tokens": float(tl.median())
            }

            self._save_stats_to_csv(
                token_stats,
                "token_statistics.csv",
                save_path
            )

        #
        # ---------------------------------------------------------
        # 2. Transformer tokenizer analysis
        # ---------------------------------------------------------
        #

        embedding_stats = {}

        embedding_models = kwargs.get(
            "embedding_models",
            []
        )
        embedding_token_series = []


        if embedding_models:

            for model_name in embedding_models:

                self.logger.info(
                    f"Analysing tokenizer limits for {model_name}"
                )

                tokenizer = AutoTokenizer.from_pretrained(
                    model_name
                )

                model_max_length = tokenizer.model_max_length

                self.logger.info(
                    f"{model_name} max length: {tokenizer.model_max_length}"
                )

                # Handle HuggingFace "unlimited" placeholder
                if model_max_length > 100000:
                    model_max_length = tokenizer.init_kwargs.get(
                        "model_max_length"
                    )

                if model_max_length is None or model_max_length > 100000:
                    self.logger.info(f"TokenizeTextEDA - {model_name} has no defined max length, defaulting to 512")
                    model_max_length = 512

                model_results = {}

                for col in cols:

                    hf_lengths = self._compute_token_lengths(
                        data,
                        col,
                        tokenizer_type="huggingface",
                        tokenizer=tokenizer
                    )

                    embedding_token_series.append(
                        pd.DataFrame({
                            "EmbeddingModel": model_name,
                            "TokenCount": hf_lengths
                        })
                    )

                    result = {
                        "min_tokens": int(hf_lengths.min()),
                        "max_tokens": int(hf_lengths.max()),
                        "avg_tokens": float(hf_lengths.mean()),
                        "median_tokens": float(hf_lengths.median()),
                        "context_window": model_max_length,
                        "documents_exceeding": None,
                        "percentage_exceeding": None
                    }

                    if model_max_length:
                        exceeding = (
                                hf_lengths > model_max_length
                        )

                        result["documents_exceeding"] = int(exceeding.sum())

                        result["percentage_exceeding"] = float(
                            exceeding.mean() * 100
                        )


                    model_results[col] = result

                embedding_stats[model_name] = model_results

            if embedding_stats:
                self._save_stats_to_csv(
                    embedding_stats,
                    "embedding_token_statistics.csv",
                    save_path
                )

            if embedding_token_series:
                embedding_boxplot_df = pd.concat(
                    embedding_token_series,
                    ignore_index=True
                )

                embedding_model_labels = kwargs.get(
                    "model_labels",
                    {}
                )

                self.logger.info(f"Embedding model labels: {embedding_model_labels}")
                if embedding_model_labels:
                    embedding_boxplot_df["EmbeddingModelLabel"] = (
                        embedding_boxplot_df["EmbeddingModel"]
                        .map(embedding_model_labels)
                        .fillna(embedding_boxplot_df["EmbeddingModel"])
                    )
                else:
                    self.logger.info("Embedding model labels not provided, using model names as labels")
                    embedding_boxplot_df["EmbeddingModelLabel"] = (
                        embedding_boxplot_df["EmbeddingModel"]
                    )

        #
        # ---------------------------------------------------------
        # 3. Visualisation configuration
        # ---------------------------------------------------------
        #

        viz_cfg = (
                viz_params
                or kwargs.get("viz_params")
                or kwargs.get("viz")
        )

        if viz_cfg is None:
            visualisations = []

        elif isinstance(viz_cfg, list):
            visualisations = viz_cfg

        elif isinstance(viz_cfg, dict):
            visualisations = viz_cfg.get(
                "visualisations",
                [viz_cfg]
            )

        else:
            visualisations = []

        if not visualisations:
            visualisations = [
                {
                    "name": "bar_chart",
                    "filename": "token_stats.png"
                }
            ]

        output_files = []

        #
        # ---------------------------------------------------------
        # 4. Existing visualisation logic
        # ---------------------------------------------------------
        #

        for vc in visualisations:

            if not isinstance(vc, dict):
                continue

            vis_name = (
                    vc.get("name")
                    or vc.get("type")
                    or "bar_chart"
            )

            if vis_name.lower() in (
                    "box_plot",
                    "boxplot",
                    "box-plot"
            ):
                vis_name = "boxplot"


            elif vis_name.lower() in (
                    "bar_chart",
                    "bar-chart"
            ):
                vis_name = "bar_chart"

            filename = (
                    vc.get("filename")
                    or f"token_stats_{vis_name}.png"
            )

            init_kwargs = {
                k: v
                for k, v in vc.items()
                if k not in (
                    "name",
                    "filename",
                    "ncols"
                )
            }

            #
            # Boxplot
            #
            if vis_name == "boxplot":

                fig, ax = plt.subplots(
                    figsize=(8, 5)
                )

                box_viz = VisualisationFactory.get_visualisation(
                    "boxplot",
                    ylabel="Token count",
                    **init_kwargs
                )

                if embedding_models:
                    box_viz = VisualisationFactory.get_visualisation(
                        "boxplot",
                        ylabel="Token count",
                        x_column="EmbeddingModelLabel",
                        y_column="TokenCount",
                        **init_kwargs
                    )
                    self.logger.info(f"Embedding boxplot Value counts: {embedding_boxplot_df["EmbeddingModel"].value_counts()}")
                    self.logger.info(f"Embedding boxplot DataFrame head:\n{embedding_boxplot_df.head()}")
                    self.logger.info(f"Embedding boxplot Unique Models: {embedding_boxplot_df["EmbeddingModelLabel"].unique()}")


                    box_viz.plot(
                        data=embedding_boxplot_df,
                        ax=ax,
                        #title="Embedding Model Token Distributions"
                    )
                else:
                    for col in cols:
                        box_viz.plot(
                            data=token_series[col],
                            ax=ax,
                            #title=col
                        )

                fig.tight_layout()

                path = os.path.join(
                    save_path,
                    filename
                )

                box_viz.save(
                    fig,
                    path
                )

                output_files.append(path)

                plt.close(fig)


            #
            # Bar chart
            #
            elif vis_name == "bar_chart":

                stats = [
                    "min_tokens",
                    "max_tokens",
                    "avg_tokens",
                    "median_tokens"
                ]

                fig, axes = plt.subplots(
                    2,
                    2,
                    figsize=(10, 8)
                )

                axes = axes.flatten()

                bar_viz = VisualisationFactory.get_visualisation(
                    "bar_chart",
                    **init_kwargs
                )

                for i, stat in enumerate(stats):
                    values = {
                        col: token_stats[col][stat]
                        for col in cols
                    }

                    bar_viz.plot(
                        data=values,
                        ax=axes[i],
                        title=stat
                    )

                fig.tight_layout()

                path = os.path.join(
                    save_path,
                    filename
                )

                bar_viz.save(
                    fig,
                    path
                )

                output_files.append(path)

                plt.close(fig)

        return {
            "token_stats": token_stats,
            "embedding_token_stats": embedding_stats,
            "visualisations": output_files
        }

    def _save_stats_to_csv(self, stats, filename, save_path):
        """
        Save token statistics dictionary to CSV.

        Parameters
        ----------
        stats : dict
            Dictionary containing token statistics.
            Supports both:
            {
                "DocumentText": {
                    "min_tokens": ...,
                    "max_tokens": ...
                }
            }

            and:

            {
                "model_name": {
                    "DocumentText": {
                        "min_tokens": ...
                    }
                }
            }

        filename : str
            Output CSV filename.

        save_path : str
            Directory where CSV should be saved.

        Returns
        -------
        str
            Path to saved CSV file.
        """

        os.makedirs(save_path, exist_ok=True)

        rows = []

        # Case 1: embedding model structure
        # {
        #   model: {
        #       column: stats
        #   }
        # }
        if all(
                isinstance(v, dict)
                and any(isinstance(x, dict) for x in v.values())
                for v in stats.values()
        ):

            for model, columns in stats.items():

                for column, values in columns.items():
                    rows.append(
                        {
                            "model": model,
                            "column": column,
                            **values
                        }
                    )

        # Case 2: normal token stats
        # {
        #   column: stats
        # }
        else:

            for column, values in stats.items():
                rows.append(
                    {
                        "column": column,
                        **values
                    }
                )

        df = pd.DataFrame(rows)

        output_file = os.path.join(
            save_path,
            filename
        )

        df.to_csv(
            output_file,
            index=False
        )

        self.logger.info(
            f"Saved token statistics to {output_file}"
        )

        return output_file
    # def run(self, data, target=None, text_field=None, save_path=None, viz_params=None, **kwargs):
    #     if save_path is None:
    #         save_path = os.getcwd()
    #     if data is None or not isinstance(data, pd.DataFrame):
    #         raise ValueError("TokenizeTextEDA requires a pandas DataFrame")
    #
    #     tokenizer_type = kwargs.get(
    #         "tokenizer_type",
    #         "lightweight"
    #     )
    #
    #     models = kwargs.get("models", [])
    #
    #     cols = kwargs.get('columns') or []
    #     if isinstance(cols, str):
    #         cols = [cols]
    #     try:
    #         cols = [c for c in list(cols) if c in data.columns]
    #     except Exception:
    #         cols = []
    #
    #     if not cols:
    #         self.logger.error("No columns specified or none found in DataFrame for TokenizeTextEDA")
    #         raise ValueError("No columns specified or none found in DataFrame for TokenizeTextEDA")
    #
    #     # compute token lengths and basic stats
    #     token_stats = {}
    #     token_series = {}
    #     for col in cols:
    #         tl = self._compute_token_lengths(data, col)
    #         token_series[col] = tl
    #         token_stats[col] = {
    #             'min_tokens': int(tl.min()) if not tl.dropna().empty else 0,
    #             'max_tokens': int(tl.max()) if not tl.dropna().empty else 0,
    #             'avg_tokens': float(tl.mean()) if not tl.dropna().empty else 0.0,
    #             'median_tokens': float(tl.median()) if not tl.dropna().empty else 0.0,
    #         }
    #
    #     # normalize viz_params
    #     viz_cfg = viz_params or kwargs.get('viz_params') or kwargs.get('viz')
    #     if viz_cfg is None:
    #         visualisations = []
    #     elif isinstance(viz_cfg, list):
    #         visualisations = viz_cfg
    #     elif isinstance(viz_cfg, dict):
    #         visualisations = viz_cfg.get('visualisations', [viz_cfg])
    #     else:
    #         visualisations = []
    #
    #     if not visualisations:
    #         # default to bar chart of stats
    #         base_fname = kwargs.get('filename') or 'token_stats.png'
    #         visualisations = [{'name': 'bar_chart', 'filename': base_fname}]
    #
    #     os.makedirs(save_path, exist_ok=True)
    #     output_files = []
    #
    #     for vc in visualisations:
    #         if not isinstance(vc, dict):
    #             self.logger.warning(f"Skipping invalid viz config: {vc}")
    #             continue
    #         vis_name = vc.get('name') or vc.get('type') or 'bar_chart'
    #         # support alias
    #         if isinstance(vis_name, str) and vis_name.lower() in ('box_plot', 'boxplot', 'box-plot'):
    #             vis_name = 'boxplot'
    #         if isinstance(vis_name, str) and vis_name.lower() in ('bar_chart', 'bar-chart'):
    #             vis_name = 'bar_chart'
    #
    #         # collect safe init kwargs
    #         init_kwargs = {
    #             k: v for k, v in vc.items()
    #             if k not in ('name', 'filename', 'ncols')
    #         }
    #
    #         filename = vc.get('filename') or kwargs.get('filename') or f"token_stats_{vis_name}.png"
    #
    #         # For bar_chart: create subplots for each statistic (min,max,avg,median)
    #         if vis_name == 'bar_chart':
    #             stats = ['min_tokens', 'max_tokens', 'avg_tokens', 'median_tokens']
    #             total = len(stats)
    #             try:
    #                 ncols = int(vc.get('ncols', kwargs.get('ncols', 2)))
    #             except Exception:
    #                 ncols = 2
    #             if ncols < 1:
    #                 ncols = 1
    #             nrows = int(math.ceil(total / ncols))
    #             fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(4 * ncols, 3.5 * nrows))
    #             if not hasattr(axes, 'flatten'):
    #                 axes = [axes]
    #             else:
    #                 axes = axes.flatten()
    #
    #             bar_viz = VisualisationFactory.get_visualisation('bar_chart', title=None, ylabel=None, **init_kwargs)
    #
    #             for i, stat in enumerate(stats):
    #                 ax = axes[i]
    #                 # build mapping col->value
    #                 mapping = {col: token_stats[col][stat] for col in cols}
    #                 try:
    #                     bar_viz.plot(data=mapping, ax=ax, title=stat)
    #                 except Exception as e:
    #                     self.logger.warning(f"Failed to plot bar chart for stat {stat}: {e}")
    #                     ax.text(0.5, 0.5, f"Error {stat}", ha='center')
    #
    #             # hide unused axes
    #             for j in range(total, len(axes)):
    #                 try:
    #                     axes[j].set_visible(False)
    #                 except Exception:
    #                     pass
    #
    #             fig.tight_layout()
    #             outpath = os.path.join(save_path, filename)
    #             try:
    #                 bar_viz.save(fig, outpath)
    #                 output_files.append(outpath)
    #                 plt.close(fig)
    #                 self.logger.info(f"Saved token stats bar chart to {outpath}")
    #             except Exception as e:
    #                 self.logger.exception(f"Failed to save token stats bar chart: {e}")
    #
    #         elif vis_name == 'boxplot':
    #             # Create boxplots of token length distributions: one subplot per column
    #             total = len(cols)
    #             try:
    #                 ncols = int(vc.get('ncols', kwargs.get('ncols', 1)))
    #             except Exception:
    #                 ncols = 1
    #             if ncols < 1:
    #                 ncols = 1
    #             nrows = int(math.ceil(total / ncols)) if total > 0 else 1
    #             fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(4 * ncols, 3.5 * nrows))
    #             if not hasattr(axes, 'flatten'):
    #                 axes = [axes]
    #             else:
    #                 axes = axes.flatten()
    #
    #             #box_viz = VisualisationFactory.get_visualisation('boxplot', title=None, ylabel='Token count')
    #
    #             box_kwargs = dict(init_kwargs)
    #
    #             if "ylabel" not in box_kwargs:
    #                 box_kwargs["ylabel"] = "Token count"
    #
    #             box_viz = VisualisationFactory.get_visualisation(
    #                 "boxplot",
    #                 **box_kwargs
    #             )
    #
    #             for i, col in enumerate(cols):
    #                 ax = axes[i]
    #                 try:
    #                     series = token_series[col].dropna()
    #                     box_viz.plot(data=series, ax=ax, title=col)
    #                 except Exception as e:
    #                     self.logger.warning(f"Failed to create boxplot for column {col}: {e}")
    #                     ax.text(0.5, 0.5, f"Error {col}", ha='center')
    #
    #             # hide unused axes
    #             for j in range(total, len(axes)):
    #                 try:
    #                     axes[j].set_visible(False)
    #                 except Exception:
    #                     pass
    #
    #             fig.tight_layout()
    #             outpath = os.path.join(save_path, filename)
    #             try:
    #                 box_viz.save(fig, outpath)
    #                 output_files.append(outpath)
    #                 plt.close(fig)
    #                 self.logger.info(f"Saved token distributions boxplot to {outpath}")
    #             except Exception as e:
    #                 self.logger.exception(f"Failed to save token distributions boxplot: {e}")
    #
    #         else:
    #             self.logger.warning(f"Visualisation '{vis_name}' not supported for TokenizeTextEDA; skipping")
    #
    #     return {'token_stats': token_stats, 'visualisations': output_files}
