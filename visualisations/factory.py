#visualisations/factory.py
from logs.logger import get_logger

class VisualisationFactory:

    _registry = {}
    logger  = get_logger("VisualisationFactory")

    @classmethod
    def register_visualisation(cls, name: str, viz_cls):
        cls._registry[name] = viz_cls
        cls.logger.info(f"Registered visualisation: {name}")


    @classmethod
    def get_visualisation(cls, name: str, **kwargs):
        cls.logger.info(f"Retrieving visualisation class for name: {name}")
        viz_cls = cls._registry.get(name)

        if not viz_cls:
            cls.logger.warning(f"Visualisation '{name}' not found in registry")
            return None

        cls.logger.info(f"Instantiating visualisation '{name}'")
        return viz_cls(**kwargs)