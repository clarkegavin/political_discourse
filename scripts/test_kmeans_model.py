from pathlib import Path
import sys
root = Path(__file__).resolve().parents[1]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

import numpy as np
from models import ModelFactory

# instantiate kmeans via factory
model = ModelFactory.get_model('kmeans', n_clusters=3, random_state=0)
print('Model created:', model)

# create dummy data
X = np.random.RandomState(0).rand(20, 2)
labels = model.fit_predict(X)
print('Labels:', labels)

