import os, sys
sys.path.insert(0, os.getcwd())
import pandas as pd
from encoders import OneHotEncoder, MultiHotEncoder

# OneHot test
s = pd.Series(['cat','dog','cat','mouse', None], name='pet')
enc = OneHotEncoder()
df = enc.fit_transform(s)
print('OneHot columns:', list(df.columns))
print(df.head())

# MultiHot test
m = pd.Series(['win, mac','linux, win','mac','linux, mac, win', None], name='platforms')
menc = MultiHotEncoder(sep=',')
df2 = menc.fit_transform(m)
print('MultiHot columns:', list(df2.columns))
print(df2.head())

