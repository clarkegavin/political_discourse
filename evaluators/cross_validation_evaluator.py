from evaluators.base import Evaluator
from sklearn.model_selection import cross_validate
import numpy as np
from logs.logger import get_logger

class CrossValidationEvaluator(Evaluator):
    """
    Evaluator that performs k-fold cross-validation on the training set
    AND standard evaluation on the test set.
    """

    def __init__(self, name: str, metrics: list, cv_folds: int = 5, **kwargs):
        self.name = name
        self.metrics = metrics
        self.cv_folds = cv_folds
        self.logger = get_logger(f"CrossValidationEvaluator:{name}")

    def evaluate(self, model, X_train, y_train, X_test, y_test, prefix="") -> dict:
        results = {}

        # --- 1. Run Cross-validation ---------------------------------------
        self.logger.info(f"Running {self.cv_folds}-fold CV with metrics: {self.metrics}")

        scoring = {m: m for m in self.metrics}  # sklearn scoring dict format
        cv_results = cross_validate(
            model, X_train, y_train,
            cv=self.cv_folds,
            scoring=scoring,
            return_train_score=False,
            n_jobs=-1
        )

        # Record fold means
        for metric in self.metrics:
            results[f"cv_{metric}"] = float(np.mean(cv_results[f"test_{metric}"]))

        # --- 2. Standard evaluation on the test set -------------------------
        self.logger.info(f"Evaluating final model on test set for metrics: {self.metrics}")

        y_pred = model.predict(X_test)

        for metric in self.metrics:
            fn = Evaluator.get_metric_function(metric)
            results[f"test_{metric}"] = float(fn(y_test, y_pred))

        return results
