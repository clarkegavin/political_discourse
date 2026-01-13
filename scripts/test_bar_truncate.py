import os, sys
sys.path.insert(0, os.getcwd())
from visualisations.bar_chart import BarChart
import pandas as pd

s = pd.Series(['ThisIsAVeryLongLabelOne','Short','AnotherVeryLongLabelTwo','MediumLen'])
bar = BarChart(title='test', label_max_chars=10)
fig, ax = bar.plot(data=s)
# collect tick labels and heights
xt = [t.get_text() for t in ax.get_xticklabels()]
ys = [p.get_height() for p in ax.patches]
print('xticklabels:', xt)
print('heights:', ys)
fig.savefig('output/eda/test_bar_truncate.png', bbox_inches='tight')
print('Saved output/eda/test_bar_truncate.png')

