class ReportingFactory:

    _registry = {}

    @classmethod
    def register_report(
        cls,
        name,
        report_cls
    ):
        cls._registry[name] = report_cls


    @classmethod
    def get_report(cls, name, **kwargs):
        report_cls = cls._registry.get(name)

        if report_cls is None:
            raise KeyError(
                f"Reporting component '{name}' not registered"
            )

        return report_cls(**kwargs)