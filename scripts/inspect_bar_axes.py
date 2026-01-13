import pandas as pd
from visualisations.bar_chart import BarChart

s = pd.Series(['Windows','Mac','Windows','Linux','Mac','Windows'])
bar = BarChart(title='platforms', xticks_rotation=45)
fig, ax = bar.plot(data=s)
# Extract tick labels and y ticks
xt = [t.get_text() for t in ax.get_xticklabels()]
yt = ax.get_yticks()
print('xticklabels:', xt)
print('yticks:', yt)
# Save figure for manual inspection
fig.savefig('output/eda/inspect_platforms.png', bbox_inches='tight')
print('Saved inspect figure to output/eda/inspect_platforms.png')

