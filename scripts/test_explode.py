import pandas as pd
from preprocessing.explode_columns import ExplodeColumns

def main():
    df = pd.DataFrame({
        'AppId':[1,2],
        'Platforms':['Windows, Mac', 'Linux, Windows,    Mac'],
        'Name':['GameA','GameB']
    })
    exp = ExplodeColumns(columns=['Platforms'], sep=',')
    res = exp.transform(df)
    res.to_csv('output/eda/explode_test.csv', index=False)
    print('WROTE output/eda/explode_test.csv')

if __name__ == '__main__':
    main()

