#visualisations/base.py
from abc import ABC, abstractmethod
from logs.logger import get_logger
import os

class Visualisation(ABC):
    """
    Abstract base class for visualisations.
    """
    def __init__(self, title: str, figsize: tuple=(10,6)):
        self.title = title
        self.logger = get_logger(f"Visualisation:{title}")
        self.logger.info(f'Initialized visualisation: {title}')

    @abstractmethod
    def plot(self, data, **kwargs):
        """
        Create the visualisation.
        """
        pass


    def save(self, obj, filepath: str, dpi=300):
        """
        Save the visualisation to a file.
        """
        # create directory if it doesn't exist
        self.logger.info(f"Preparing to save visualisation to {filepath}")
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self.logger.info(f'Saving visualisation to {filepath}')
        # fig.savefig(filepath, dpi=dpi, bbox_inches='tight')
        # self.logger.info(f'Visualisation saved to {filepath}')
        ext = os.path.splitext(filepath)[1].lower()
        self.logger.info(f"File extension determined: {ext}")

        # ---------------------------
        # MATPLOTLIB SAVE
        # ---------------------------
        if hasattr(obj, "savefig"):
            obj.savefig(filepath, dpi=dpi, bbox_inches="tight")
            self.logger.info("Saved using matplotlib backend")
            return

        if ext == ".png":
            self.logger.info("Saving using matplotlib PNG backend")
            obj.savefig(filepath, dpi=dpi, bbox_inches="tight")
            self.logger.info("Saved using matplotlib PNG backend")
            return

        # ---------------------------
        # PLOTLY SAVE
        # ---------------------------
        if ext == ".html":
            self.logger.info("Saving using plotly HTML backend")
            obj.write_html(filepath)
            self.logger.info("Saved using plotly HTML backend")
            return

        raise ValueError(f"Unsupported visualisation type or file format: {ext}")