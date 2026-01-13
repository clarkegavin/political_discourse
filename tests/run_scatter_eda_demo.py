import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from eda.scatter_eda import ScatterPlotEDA


def main():
    df = pd.DataFrame({
        'f1':[1,2,3,4,5,6],
        'f2':[2,3,4,5,6,7],
        'f3':[3,4,5,6,7,8],
        'Genre':['A','B','A','B','A','B']
    })

    eda = ScatterPlotEDA()
    out = eda.run(df, save_path='output/eda', reducer={'name':'pca', 'params': {'n_components':2}}, viz_params={'name':'cluster_plot', 'output_dir':'output/eda/scatter_demo', 'figsize':(6,4), 'title':'Demo'}, filename='demo_scatter.png', save_interactive=False, color_by='Genre')
    print('Saved scatter to', out)

if __name__=='__main__':
    main()

