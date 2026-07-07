# eda/word_cloud_eda.py

import os
from logs.logger import get_logger
from visualisations.factory import VisualisationFactory
from wordcloud import STOPWORDS


class WordCloudEDA:

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    def _build_text(self, df, columns):
        return (
            df[columns]
            .fillna("")
            .astype(str)
            .agg(" ".join, axis=1)
            .str.cat(sep=" ")
        )

    def _resolve_stopwords(self, use_stopwords: bool):
        """
        Converts YAML boolean into WordCloud-compatible stopwords set.
        """
        if use_stopwords is False:
            return set()
        return STOPWORDS

    def run(self, data, target=None, text_field=None, save_path=None, **kwargs):

        os.makedirs(save_path, exist_ok=True)

        columns = kwargs.get("columns", [])
        combine_columns = kwargs.get("combine_columns", False)
        viz_params = kwargs.get("viz_params", [])


        outputs = {}

        for viz_param in viz_params:
            viz_params = dict(viz_param)
            viz_name = viz_params.pop("name")

            # -----------------------------
            # Resolve stopwords BEFORE passing to WordCloud
            # -----------------------------
            use_stopwords = viz_params.pop("stopwords", True)
            stopwords = self._resolve_stopwords(use_stopwords)

            visualisation = VisualisationFactory.get_visualisation(
                viz_name,
                **viz_params
            )

            if visualisation is None:
                raise KeyError(f"Visualisation '{viz_name}' not registered")

            try:

                # =====================================================
                # COMBINED WORD CLOUD
                # =====================================================
                if combine_columns:

                    self.logger.info("Generating combined word cloud")

                    text = self._build_text(data, columns)

                    fig, ax = visualisation.plot(
                        text=text,
                        stopwords=stopwords,
                        **viz_params
                    )

                    filename = viz_params.get(
                        "filename",
                        "wordcloud_combined.png"
                    )

                    outpath = os.path.join(save_path, filename)

                    visualisation.save(fig, outpath)

                    outputs[viz_name] = outpath

                # =====================================================
                # PER-COLUMN WORD CLOUDS
                # =====================================================
                else:

                    self.logger.info("Generating per-column word clouds")

                    column_outputs = {}

                    for col in columns:
                        text = self._build_text(data, [col])

                        fig, ax = visualisation.plot(
                            text=text,
                            stopwords=stopwords,
                            **viz_params
                        )

                        filename = viz_params.get(
                            "filename",
                            f"wordcloud_{col}.png"
                        ).replace(" ", "_")

                        outpath = os.path.join(save_path, filename)

                        visualisation.save(fig, outpath)

                        column_outputs[col] = outpath

                    outputs[viz_name] = column_outputs

            except Exception as e:
                self.logger.warning(f"WordCloud failed: {e}")

        return outputs