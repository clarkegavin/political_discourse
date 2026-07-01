import sys, os
sys.path.insert(0, os.getcwd())

import pandas as pd
from eda.missing_values_eda import MissingValuesEDA

def main():
    df = pd.DataFrame({
        'Questioner': ['A', 'A', 'B', 'C', None],
        'QuestionerParty': ['P1', 'P1', 'P2', 'P2', 'P1'],
        'AnswerText': ['Yes', '', None, 'OK', '   ']
    })

    m = MissingValuesEDA()
    print(m.compute_missingness_rate(df, 'Questioner', 'AnswerText'))
    print(m.compute_missingness_rate(df, 'QuestionerParty', 'AnswerText'))

if __name__ == '__main__':
    main()
