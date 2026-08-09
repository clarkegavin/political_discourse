import os

from logs.logger import get_logger


class LatexSummaryTable:

    def __init__(self):

        self.logger = get_logger(
            self.__class__.__name__
        )

    def run(
        self,
        data,
        output_path,
        **kwargs
    ):

        if data is None:

            raise ValueError(
                "LatexSummaryTable requires reporting data."
            )

        latex = self._generate_table(
            data,
            kwargs
        )

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
        data,
        config
    ):

        data = self._sort_data(
            data,
            config
        )

        columns = config[
            "columns"
        ]

        # ---------------------------------
        # Validate configured columns
        # ---------------------------------

        missing_columns = [
            column["field"]
            for column in columns
            if column["field"] not in data.columns
        ]

        if missing_columns:

            raise ValueError(
                "Configured table columns are missing "
                f"from reporting data: {missing_columns}. "
                f"Available fields: {list(data.columns)}"
            )

        # ---------------------------------
        # LaTeX column specification
        # ---------------------------------

        latex_columns = "".join(
            [
                column.get(
                    "latex_type",
                    "l"
                )
                for column in columns
            ]
        )

        # ---------------------------------
        # Headers
        # ---------------------------------

        headers = " & ".join(
            [
                (
                    "\\textbf{"
                    f"{self._escape_latex(column['header'])}"
                    "}"
                )
                for column in columns
            ]
        )

        # ---------------------------------
        # Determine highlighted values
        # ---------------------------------

        highlights = self._get_highlight_values(
            data,
            config
        )

        # ---------------------------------
        # Generate rows
        # ---------------------------------

        rows = []

        for _, row in data.iterrows():

            values = []

            for column in columns:

                field = column[
                    "field"
                ]

                value = row[
                    field
                ]

                formatted_value = (
                    self._format_value(
                        value,
                        column
                    )
                )

                # ---------------------------------
                # Highlight value
                # ---------------------------------

                if self._should_highlight(
                    row,
                    column,
                    highlights
                ):

                    formatted_value = (
                        "\\textbf{"
                        f"{formatted_value}"
                        "}"
                    )

                values.append(
                    formatted_value
                )

            rows.append(
                " & ".join(values)
                + r" \\"
            )

        # ---------------------------------
        # Generate LaTeX
        # ---------------------------------

        return f"""
\\begin{{table}}[!htbp]
\\centering
\\begin{{scriptsize}}
\\caption{{{config.get('caption', '')}}}
\\label{{{config.get('label', '')}}}

\\begin{{tabular}}{{{latex_columns}}}

\\toprule

{headers}

\\\\

\\midrule

{chr(10).join(rows)}

\\bottomrule

\\end{{tabular}}

\\end{{scriptsize}}
\\end{{table}}
"""

    def _escape_latex(
        self,
        value
    ):

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

        value = str(
            value
        )

        for char, replacement in (
            replacements.items()
        ):

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

                value = (
                    f"{float(value):.{precision}f}"
                )

            except (
                ValueError,
                TypeError
            ):

                pass

        return self._escape_latex(
            value
        )

    def _sort_data(
        self,
        data,
        config
    ):

        self.logger.info(
            "Sorting data"
        )

        sort_config = config.get(
            "sort"
        )

        if not sort_config:

            return data

        field = sort_config[
            "field"
        ]

        descending = sort_config.get(
            "descending",
            True
        )

        if field not in data.columns:

            raise ValueError(
                f"Sort field '{field}' "
                f"not found in reporting data. "
                f"Available fields: "
                f"{list(data.columns)}"
            )

        return data.sort_values(
            by=field,
            ascending=not descending,
            na_position="last"
        )

    def _should_highlight(
        self,
        row,
        column,
        highlights
    ):

        field = column[
            "field"
        ]

        if field not in highlights:

            return False

        return (
            row.get(
                "Run ID"
            )
            ==
            highlights[field][
                "run_id"
            ]
        )

    def _get_highlight_values(
        self,
        data,
        config
    ):
        """
        Identify which rows should be highlighted
        for each configured metric.

        Returns:
            {
                "Coherence": {
                    "run_id": "...",
                    "value": 0.452
                }
            }
        """

        highlights = {}

        # ---------------------------------
        # Determine run ID column
        # ---------------------------------

        run_id_field = config.get(
            "run_id_field",
            "Run ID"
        )

        if run_id_field not in data.columns:

            raise ValueError(
                f"Run ID field '{run_id_field}' "
                f"not found in reporting data. "
                f"Available fields: "
                f"{list(data.columns)}"
            )

        # ---------------------------------
        # Process highlight rules
        # ---------------------------------

        for rule in config.get(
            "highlight",
            []
        ):

            field = rule[
                "field"
            ]

            direction = rule.get(
                "direction",
                "max"
            )

            if field not in data.columns:

                self.logger.warning(
                    "Highlight field '%s' "
                    "not found in reporting data. "
                    "Skipping.",
                    field
                )

                continue

            # ---------------------------------
            # Convert values to numeric
            # ---------------------------------

            candidates = (
                data[
                    [
                        run_id_field,
                        field
                    ]
                ]
                .copy()
            )

            candidates[
                field
            ] = candidates[
                field
            ].apply(
                self._to_float
            )

            candidates = candidates.dropna(
                subset=[
                    field
                ]
            )

            if candidates.empty:

                self.logger.warning(
                    "No numeric values available "
                    "for highlight field '%s'.",
                    field
                )

                continue

            # ---------------------------------
            # Select best value
            # ---------------------------------

            if direction == "max":

                selected = candidates.loc[
                    candidates[field].idxmax()
                ]

            elif direction == "min":

                selected = candidates.loc[
                    candidates[field].idxmin()
                ]

            else:

                raise ValueError(
                    "Unsupported highlight "
                    f"direction: {direction}"
                )

            highlights[field] = {
                "run_id": selected[
                    run_id_field
                ],
                "value": selected[
                    field
                ]
            }

        return highlights

    @staticmethod
    def _to_float(
        value
    ):

        try:

            return float(
                value
            )

        except (
            ValueError,
            TypeError
        ):

            return None