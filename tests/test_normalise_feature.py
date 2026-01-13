import pandas as pd
from preprocessing.normalise_feature import NormaliseFeature


def test_normalise_feature_basic():
    df = pd.DataFrame({
        'num_Supported_Languages': [10, 5, 0, None],
        'num_Genres': [2, 1, 0, 3]
    })

    nf = NormaliseFeature(numerator='num_Supported_Languages', denominator='num_Genres', denom_transform='log1p', smoothing=1e-9, post_log=True)
    out = nf.transform(df)

    assert 'num_Supported_Languages_per_num_Genres' in out.columns
    res = out['num_Supported_Languages_per_num_Genres']
    # First row: num=10, denom=2 -> denom_trans=log1p(2)=~1.0986 -> ratio=10/1.0986=~9.1 -> log1p->~2.31
    assert not pd.isna(res.iloc[0])
    # third row: denom=0 -> denom_trans=log1p(0)=0 -> denom + smoothing -> small -> ratio large but finite -> logged
    assert not pd.isna(res.iloc[2])


if __name__ == '__main__':
    test_normalise_feature_basic()
    print('test passed')

