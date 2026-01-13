import pandas as pd
from filters.filter_rows import FilterRows

print('Running FilterRows smoke test')

df = pd.DataFrame({
    'AppID': [1,2,3,4],
    'Release_Date': ['2024-01-01','2025-12-31','2026-01-01', None],
    'Score': [10, 20, 30, 40]
})

print('Input DF:')
print(df)

# Example: exclude rows where Release_Date > 2025-12-31
f = FilterRows(field='Release_Date', values='2025-12-31', operator='gt', include=False)
out = f.transform(df)
print('\nAfter exclude gt 2025-12-31 (should remove AppID 3):')
print(out)

# Example: include only rows where Release_Date is null
f2 = FilterRows(field='Release_Date', values=[None], operator='in', include=True)
out2 = f2.transform(df)
print('\nInclude rows where Release_Date is null (should keep AppID 4):')
print(out2)

# Numeric comparison example
f3 = FilterRows(field='Score', values=25, operator='gte', include=True)
out3 = f3.transform(df)
print('\nInclude rows where Score >= 25 (should keep AppID 3 and 4):')
print(out3)

