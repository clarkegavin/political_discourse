import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from imputers.simple_imputer import SimpleImputer


def main():
    df = pd.DataFrame({
        'Is_Free': [1, 1, 0, 0, 0],
        'Current_Players': [None, 0, 10, None, 5],
        'Price_Final': [0.0, 0.0, 9.99, None, 4.99]
    })

    print('Original:\n', df)

    # Imputer for free games: fill numeric with zero where Is_Free == 1
    imp_free = SimpleImputer(columns=['Current_Players','Price_Final'], numeric_strategy='zero', filter_column='Is_Free', filter_value=1)
    imp_free.fit(df)
    df_after_free = imp_free.transform(df)
    print('\nAfter imputer (Is_Free==1 -> zero):\n', df_after_free)

    # Imputer for non-free games: fill numeric with mean where Is_Free == 0
    imp_paid = SimpleImputer(columns=['Current_Players','Price_Final'], numeric_strategy='mean', filter_column='Is_Free', filter_value=0)
    imp_paid.fit(df_after_free)
    df_after_paid = imp_paid.transform(df_after_free)
    print('\nAfter imputer (Is_Free==0 -> mean):\n', df_after_paid)

    # Show final values per row
    print('\nFinal DataFrame:\n', df_after_paid)

if __name__ == '__main__':
    main()

