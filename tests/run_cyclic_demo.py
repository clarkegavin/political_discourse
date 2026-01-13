import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from preprocessing.cyclic_encode import CyclicEncode


def main():
    df = pd.DataFrame({
        'release_month': [1, 6, 12, None, 3],
        'release_weekday': [1, 4, 7, 2, None]
    })

    print('Original:\n', df)

    ce = CyclicEncode(columns=['release_month', 'release_weekday'], period={'release_month':12, 'release_weekday':7}, drop_original=True)
    out = ce.transform(df)
    print('\nTransformed:\n', out)

if __name__ == '__main__':
    main()

