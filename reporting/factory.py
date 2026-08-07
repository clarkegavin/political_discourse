from typing import Dict


class ReportingFactory:

    _registry = {}


    @classmethod
    def register_report(
        cls,
        name,
        report
    ):

        cls._registry[name] = report


    @classmethod
    def get_report(
        cls,
        name
    ):

        if name not in cls._registry:
            raise KeyError(
                f"Reporting component '{name}' not registered"
            )

        return cls._registry[name]