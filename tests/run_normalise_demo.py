import sys
from pathlib import Path
# ensure project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from preprocessing.normalise_feature import NormaliseFeature


def main():
    df = pd.DataFrame({
        'num_Supported_Languages': [10, 5, 0, None],
        'num_Genres': [2, 1, 0, 3]
    })

    nf = NormaliseFeature(numerator='num_Supported_Languages', denominator='num_Genres', denom_transform='log1p', smoothing=1e-9, post_log=True)
    out = nf.transform(df)

    print('Columns:', out.columns.tolist())
    print(out)
    assert 'num_Supported_Languages_per_num_Genres' in out.columns
    print('NormaliseFeature demo: OK')


if __name__ == '__main__':
    main()

