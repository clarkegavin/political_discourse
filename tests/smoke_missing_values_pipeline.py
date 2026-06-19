import sys, os
sys.path.insert(0, os.getcwd())
from eda.missing_values_eda import MissingValuesEDA
import pandas as pd


def main():
    df = pd.DataFrame({
        'Questioner': ['A','A','B','C', None, 'B'],
        'QuestionerParty': ['P1','P1','P2','P2','P1','P2'],
        'AnswerText': ['Yes','','No',None,'OK','  ']
    })

    m = MissingValuesEDA()
    viz_params = [{'name':'box_plot','filename':'pipeline_missing.png'}]
    out = m.run(df, save_path='output_test', columns=['Questioner','QuestionerParty'], missing_values_column='AnswerText', consolidate=True, viz_params=viz_params)
    print('run returned:', out)
    for p in out.get('visualisations', []):
        print(p, os.path.exists(p))

if __name__ == '__main__':
    main()

