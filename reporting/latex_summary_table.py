import os

from logs.logger import get_logger
from reporting.mlflow_reader import MLflowReader


class LatexSummaryTable:


    def __init__(self):
        self.logger = get_logger(
            self.__class__.__name__
        )

    def run(
        self,
        output_path,
        **kwargs
    ):
        reader = MLflowReader(tracking_uri=kwargs.get("tracking_uri"))
        runs = reader.load_runs(experiment_id=kwargs.get("experiment_id"),
            run_list=kwargs.get("run_list")
        )
        latex = self._generate_table(runs, kwargs)

        filename = kwargs.get(
            "output_file",
            "summary_table.tex"
        )

        output_file = os.path.join(
            output_path,
            filename
        )

        with open(
            output_file,
            "w",
            encoding="utf-8"
        ) as f:

            f.write(
                latex
            )

        self.logger.info(
            f"LaTeX table generated: {output_file}"
        )

        return output_file



    def _generate_table(
        self,
        runs,
        config
    ):

        runs = self._sort_runs(
            runs,
            config
        )
        columns = config["columns"]

        latex_columns = "".join(
            [
                c.get(
                    "latex_type",
                    "l"
                )

                for c in columns
            ]
        )

        headers = " & ".join(
            [
                f"\\textbf{{{self._escape_latex(c['header'])}}}"
                for c in columns
            ]
        )

        highlights = self._get_highlight_values(
            runs,
            config
        )

        rows = []

        for run in runs:
            self.logger.info(run["params"])
            self.logger.info(run["metrics"])
            values = []

            for column in columns:

                value = self._resolve_field(
                    run,
                    column["field"]
                )

                formatted = self._format_value(
                    value,
                    column
                )

                # apply bolding here
                if self._should_highlight(
                        run,
                        column,
                        highlights
                ):
                    formatted = (
                        f"\\textbf{{{formatted}}}"
                    )

                values.append(formatted)

            rows.append(
                " & ".join(values) + r" \\"
            )

        return f"""
\\begin{{table}}[!htbp]
\\centering
\\begin{{scriptsize}}
\\caption{{{config.get('caption','')}}}
\\label{{{config.get('label','')}}}

\\begin{{tabular}}{{{latex_columns}}}

\\toprule

\\textbf{headers}

\\\\

\\midrule

{chr(10).join(rows)}


\\bottomrule

\\end{{tabular}}

\\end{{scriptsize}}
\\end{{table}}
"""


    def _resolve_field(
        self,
        run,
        field
    ):

        parts = field.split(".")
        value = run
        for part in parts:
            value = value.get(
                part,
                ""
            )
        return value

    def _escape_latex(self, value):
        if value is None:
            return ""
        replacements = {
            "\\": "\\textbackslash{}",
            "_": "\\_",
            "%": "\\%",
            "&": "\\&",
            "#": "\\#",
            "{": "\\{",
            "}": "\\}",
        }
        value = str(value)

        for char, replacement in replacements.items():
            value = value.replace(
                char,
                replacement
            )

        return value

    def _format_value(
            self,
            value,
            column
    ):

        if value is None:
            return ""

        precision = column.get(
            "precision"
        )

        if precision is not None:
            try:
                value = f"{float(value):.{precision}f}"
            except (ValueError, TypeError):
                pass

        return self._escape_latex(value)

    def _sort_runs(self, runs, config):
        self.logger.info("Sorting runs")
        sort_config = config.get("sort")

        if not sort_config:
            return runs

        field = sort_config["field"]
        descending = sort_config.get(
            "descending",
            True
        )

        return sorted(
            runs,
            key=lambda x: self._resolve_field(
                x,
                field
            ) or 0,
            reverse=descending
        )

    def _should_highlight(
            self,
            run,
            column,
            highlights
    ):
        self.logger.info("Checking if run should be highlighted")
        field = column["field"]

        if field not in highlights:
            return False

        return (
                run["run_id"]
                ==
                highlights[field]["run_id"]
        )

    def _get_highlight_values(
            self,
            runs,
            config
    ):
        """
        Identify which runs should be highlighted for each configured metric.

        Returns:
            {
                "metrics.coherence": {
                    "run_id": "...",
                    "value": 0.452
                }
            }
        """

        highlights = {}

        for rule in config.get(
                "highlight",
                []
        ):

            field = rule["field"]
            direction = rule.get(
                "direction",
                "max"
            )

            candidates = []

            for run in runs:

                value = self._resolve_field(
                    run,
                    field
                )

                try:
                    candidates.append(
                        (
                            run["run_id"],
                            float(value)
                        )
                    )

                except (ValueError, TypeError):
                    continue

            if not candidates:
                continue

            if direction == "max":

                selected = max(
                    candidates,
                    key=lambda x: x[1]
                )

            elif direction == "min":

                selected = min(
                    candidates,
                    key=lambda x: x[1]
                )

            else:

                raise ValueError(
                    f"Unsupported highlight direction: {direction}"
                )

            highlights[field] = {
                "run_id": selected[0],
                "value": selected[1]
            }

        return highlights

