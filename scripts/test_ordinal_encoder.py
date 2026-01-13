import os, sys
sys.path.insert(0, os.getcwd())
import pandas as pd
from encoders import OrdinalEncoder

s = pd.Series(['low','medium','high','low', None], name='priority')
enc = OrdinalEncoder()
df = enc.fit_transform(s)
print('Ordinal columns:', df.name if isinstance(df, pd.Series) else 'array')
print(df)
inv = enc.inverse_transform(df)
print('Inverse:', inv)

# test handle_unknown
enc2 = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-2)
enc2.fit(['a','b'])
print('transform unknown handling:', enc2.transform(['a','c','b']))

