from .base import Visualisation
from logs.logger import get_logger

import os
from typing import Optional


class BERTopicVisualisation(Visualisation):
    """
    Base class for BERTopic Plotly and text-based visualisations.

    Handles:
      - model unwrapping
      - BERTopic method invocation
      - output handling
      - HTML/text saving
    """

    def __init__(
        self,
        name: str,
        title: str = None,
        output_dir: str = ".",
        filename: str = "bertopic_visualisation.html",
        figsize=(10, 6),
        method: str = None,
        output_type: str = "figure",
        **params
    ):

        super().__init__(
            title=title,
            figsize=figsize
        )

        self.logger = get_logger(self.__class__.__name__)

        self.name = name
        self.output_dir = output_dir
        self.filename = filename
        self.method = method
        self.output_type = output_type

        # All BERTopic-specific arguments
        self.params = params

        self.logger.info(
            f"Initialised {self.name} using BERTopic method "
            f"'{self.method}' with params: {self.params}"
        )


    def _get_bertopic_model(self, model):

        if model is None:
            self.logger.warning(
                "No model supplied to BERTopic visualisation"
            )
            return None

        # unwrap your BERTopicModel wrapper
        bertopic_model = getattr(
            model,
            "model",
            model
        )

        return bertopic_model


    def _render(
        self,
        model,
        save_path: Optional[str] = None,
        filename: Optional[str] = None,
        **plot_kwargs
    ):

        bertopic_model = self._get_bertopic_model(model)

        if bertopic_model is None:
            return None


        if not hasattr(
            bertopic_model,
            self.method
        ):
            self.logger.warning(
                f"Model does not expose BERTopic method "
                f"'{self.method}'"
            )
            return None


        try:

            visualisation_method = getattr(
                bertopic_model,
                self.method
            )

            result = visualisation_method(
                **self.params,
                **plot_kwargs
            )

        except Exception as e:

            self.logger.exception(
                f"Failed generating {self.name}: {e}"
            )

            return None


        return self._save(
            result,
            save_path,
            filename
        )


    def _save(
        self,
        obj,
        save_path=None,
        filename=None
    ):

        output_dir = (
            save_path
            or self.output_dir
            or "."
        )

        output_filename = (
            filename
            or self.filename
        )


        os.makedirs(
            output_dir,
            exist_ok=True
        )


        output_path = os.path.join(
            output_dir,
            output_filename
        )


        try:

            if self.output_type == "text":

                with open(
                    output_path,
                    "w",
                    encoding="utf-8"
                ) as f:
                    f.write(str(obj))

                self.logger.info(
                    f"Saved {self.name} visualisation: "
                    f"{output_path}"
                )

                return output_path


            # Default: Plotly figure
            obj.write_html(
                output_path
            )

            self.logger.info(
                f"Saved {self.name} visualisation: "
                f"{output_path}"
            )

            return output_path


        except Exception as e:

            self.logger.exception(
                f"Could not save visualisation: {e}"
            )

            return None