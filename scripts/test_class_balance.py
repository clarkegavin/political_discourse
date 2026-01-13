import pandas as pd
from eda.class_balance_eda import ClassBalanceEDA

if __name__ == '__main__':
    df = pd.DataFrame({
        'platforms': ['Windows','Mac','Windows','Linux','Mac','Windows'],
        'Id': [1,2,3,4,5,6]
    })
    cb = ClassBalanceEDA()
    out = cb.run(df, target=None, save_path='output/eda', exclude_columns=['Id'])
    print('Saved to', out)

