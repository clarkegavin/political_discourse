from .base import EDAComponent
from logs.logger import get_logger
import os

class DuplicateCheckEDA(EDAComponent):
    """
    EDA component to check for duplicate entries in the dataset.
    """

    def __init__(self):
        self.logger = get_logger("DuplicateCheckEDA")
        self.logger.info("Initialized DuplicateCheckEDA component")

    def run(self, data, target=None, text_field=None, **kwargs):
        """
        Check for duplicate entries in the dataset.

        Parameters:
        - data: The dataset to check for duplicates.
        - target: Not used in this component.
        - text_field: Not used in this component.
        - kwargs: Additional parameters (not used).

        Returns:
        - duplicates_info: A dictionary containing information about duplicates.
        """

        self.logger.info("Checking for duplicate entries in the dataset")
        duplicate_rows = data[data.duplicated(keep=False)]
        num_duplicates = len(duplicate_rows)
        unique_duplicates = duplicate_rows.drop_duplicates()

        duplicates_info = {
            "total_duplicates": num_duplicates,
            "unique_duplicate_entries": unique_duplicates
        }

        self.logger.info(f"Found {num_duplicates} duplicate entries")
        self._save(duplicates_info, kwargs.get("save_path", "."))

        return duplicates_info

    def _save(self, duplicates_info, save_path):
        """
        Save duplicate information to a file.

        Parameters:
        - duplicates_info: The dictionary containing duplicate information.
        - save_path: The path to save the duplicate information.
        """

        filepath = os.path.join(save_path, "duplicates_info.txt")
        with open(filepath, "w", encoding="utf-8", errors="replace") as f:
            f.write(f"Total Duplicates: {duplicates_info['total_duplicates']}\n")
            f.write("Unique Duplicate Entries:\n")
            f.write(
                duplicates_info['unique_duplicate_entries']
                .to_string(index=False)
            )
        self.logger.info(f"Duplicate information saved to {filepath}")
        return filepath