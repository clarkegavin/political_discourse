import sys
from pathlib import Path
import pandas as pd

# Ensure project root is on sys.path
root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from preprocessing import RemoveRepeatedCharacters, RemovePunctuationNoise, RemoveWhitespace


def main():
    data = {
        "Id": [1, 2, 3],
        "Description": [
            "Soooo    cooool!!!!! Visit https://example.com!!!",
            "Noooooo   way...... What???!!",
            "   Leading and trailing   spaces   everywhere   "
        ]
    }
    df = pd.DataFrame(data)

    print("Original DataFrame:\n", df["Description"].to_list())

    rr = RemoveRepeatedCharacters({"field": "Description"})
    df_rr = rr.apply(df)
    print("After RemoveRepeatedCharacters:\n", df_rr["Description"].to_list())

    rp = RemovePunctuationNoise({"field": "Description"})
    df_rp = rp.apply(df_rr)
    print("After RemovePunctuationNoise:\n", df_rp["Description"].to_list())

    rw = RemoveWhitespace({"field": "Description"})
    df_rw = rw.apply(df_rp)
    print("After RemoveWhitespace:\n", df_rw["Description"].to_list())

if __name__ == '__main__':
    main()
