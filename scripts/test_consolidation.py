import pandas as pd
from eda.consolidation_eda import ConsolidationEDA

# Sample data matching the user's example
data = {
    'DebateSectionURI': [
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1033',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1033',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1034',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1034',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1084',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1084',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1084',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1087',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1087',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1087',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1087',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1087',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_100',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1003',
        'https://data.oireachtas.ie/akn/ie/debateRecord/dail/2025-01-22/writtens/dbsect_1004',
    ],
    'QuestionDate': ['2025-01-22'] * 15
}

df = pd.DataFrame(data)

eda = ConsolidationEDA()
result = eda.run(df, target='DebateSectionURI', save_path='output', filename='test_consolidation.png', date_column='QuestionDate', date_from='2025-01-01', date_to='2025-12-31', metric='groups')
print('Result:', result)
print('Saved file at:', result['filepath'])

