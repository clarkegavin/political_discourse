# experiments/classification_experiment.py
from experiments.base import Experiment
from models.factory import ModelFactory
from evaluators.factory import EvaluatorFactory
from reducers.factory import ReducerFactory
from logs.logger import get_logger
import mlflow
import json, os
from datetime import datetime
from typing import Optional, Dict, List, Any
from sklearn.model_selection import KFold, StratifiedKFold
import numpy as np
from vectorizers.factory import VectorizerFactory
from mlflow.models import infer_signature
from visualisations.factory import VisualisationFactory
from samplers.factory import SamplerFactory
from collections import Counter


class ClassificationExperiment(Experiment):
    def __init__(
        self,
        name: str,
        model_name: str,
        evaluator_name: str,
        metrics: List[str],
        save_path: Optional[str] = None,
        model_params: Optional[Dict] = None,
        evaluator_params: Optional[Dict] = None,
        mlflow_tracking: bool = True,
        mlflow_experiment: Optional[str] = None,
        description: Optional[str] = None,
        calculate_sample_weights: bool = False,
        target_encoder: Optional[Any] = None,
        vectorizer: Optional[Dict] = None,
        visualisations: Optional[List[Dict]] = None,
        preprocessing_metadata: Optional[Dict] = None,
        sampler: Optional[Dict] = None,
        reducer: Optional[Dict] = None,
        **kwargs
    ):

        super().__init__(name, mlflow_tracking, mlflow_experiment)

        # Logger
        self.logger = get_logger(f"ClassificationExperiment:{name}")
        self.logger.info("Initializing classification experiment")

        # Standard fields
        self.name = name
        self.model_name = model_name
        self.metrics = metrics
        self.description = description
        self.calculate_sample_weights = calculate_sample_weights
        self.evaluator_name = evaluator_name
        self.save_path = save_path
        self.target_encoder = target_encoder

        # --- FLEXIBLE PARAMETER HANDLING -------------------------
        # Everything not explicitly defined is inside kwargs
        # Example: n_neighbors, weights, cv_enabled, vectorizer override etc.
        self.extra_params = kwargs


        # Vectorizer
        self.vectorizer = vectorizer or {}
        self.vectorizer_name = self.vectorizer.get("vectorizer_name")
        self.vectorizer_field = self.vectorizer.get("vectorizer_field")
        self.vectorizer_params = self.vectorizer.get("vectorizer_params", {})

        self.visualisations = visualisations or []

        # Sampler
        self.sampler_config = sampler or {}
        self.sampler_name = self.sampler_config.get("name")
        self.sampler_params = self.sampler_config.get("params", {})
        #self.sampler = None
        self.sampler = sampler
        self.logger.info(f"Sampler name: {self.sampler_name}, params: {self.sampler_params}")
        # if self.sampler_name:
        #     self.sampler = SamplerFactory.get_sampler(self.sampler_name, **self.sampler_params)

        # Reducer
        self.reducer_config = reducer or {}
        self.reducer_name = self.reducer_config.get("name")
        self.reducer_params = self.reducer_config.get("params", {})
        self.reducer = reducer


        # Model parameters: merge YAML model_params + extra params that belong to the model
        self.model_params = (model_params or {}).copy()
        for k, v in kwargs.items():
            if k not in ["cv_enabled", "cv_folds", "cv_shuffle", "cv_random_state",
                         "cv_stratified", "visualisations", "vectorizer"]:
                self.model_params.setdefault(k, v)

        self.logger.info(f"Model params resolved: {self.model_params}")

        # Cross-validation params (default off unless YAML enables)
        self.cv_enabled = kwargs.get("cv_enabled", False)
        self.cv_folds = kwargs.get("cv_folds", 5)
        self.cv_stratified = kwargs.get("cv_stratified", True)
        self.cv_shuffle = kwargs.get("cv_shuffle", True)
        self.cv_random_state = kwargs.get("cv_random_state", 42)

        # Initializing model/evaluator
        self.model = ModelFactory.get_model(self.model_name, **self.model_params)
        self.evaluator = EvaluatorFactory.get_evaluator(name=evaluator_name, **(evaluator_params or {}))

        # preprocessing metadata
        self.preprocessing_metadata = preprocessing_metadata or []
        # Results store
        self.results = {}
        self.logger.info("ClassificationExperiment initialised successfully.")

    def run(self, X_train, X_test, y_train, y_test):
        self.logger.info(f"Running classification experiment '{self.name}'")

               # --- 1. Vectorizer support -----------------------

        with mlflow.start_run(run_name=self.name):
            if self.vectorizer_name:
                self.logger.info(f"Using vectorizer '{self.vectorizer_name}' on field '{self.vectorizer_field}'")
                self.vectorizer_params['column'] = self.vectorizer_field
                vectorizer = VectorizerFactory.get_vectorizer(
                    self.vectorizer_name,
                    **self.vectorizer_params
                )
                X_train = vectorizer.fit_transform(X_train.fillna(""))  # Fit on training set
                X_test = vectorizer.transform(X_test.fillna(""))  # Transform on test set


            self._log_mlflow_params()

            if self.sampler_name:
                self._map_sampler_strategy_to_numeric()

                self.logger.info(
                    f"Instantiating sampler '{self.sampler_name}' with params {self.sampler_params}"
                )
                self.sampler = SamplerFactory.get_sampler(
                    self.sampler_name, **self.sampler_params
                )
            else:
                self.logger.info("No sampler configured.")


            # Reducer
            if self.reducer:
                self.logger.info(f"Using reducer '{self.reducer_name}' with params {self.reducer_params}")
                reducer = ReducerFactory.create_reducer(self.reducer_name, **self.reducer_params)
                X_train = reducer.fit_transform(X_train)
                X_test = reducer.transform(X_test)
                self.logger.info("Dimensionality reduction complete.")

            if self.cv_enabled:
                self.results = self._run_cross_validation(X_train, y_train)

            # --- Apply sampler to full training set before final training ---
            # if self.sampler is not None:
                # self.logger.info(f"Applying sampler '{self.sampler_name}' to full training set")
                # X_train, y_train = self.sampler.fit_resample(X_train, y_train)

            if self.sampler is not None:
                self.logger.info(f"Applying sampler '{self.sampler_name}' to full training set")
                X_train, y_train = self.sampler.fit_resample(X_train, y_train)

            # --- Train final model on full training set ---

            self.logger.info("Training final model on full training set")
            final_model = ModelFactory.get_model(self.model_name, **self.model_params)
            #final_model.fit(X_train, y_train)

            # compute sample weights for xgboost if needed
            if self.calculate_sample_weights:  # will only work for some models, but is only currently controlled through the yaml configuration
                self.logger.info("Computing sample weights for full training set")
                sample_weights = self._compute_sample_weights(y_train)
                self.logger.info(f"Sample weights computed: {sample_weights}")
                final_model.fit(X_train, y_train, sample_weight=sample_weights)
            else:
                self.logger.info("Training final model on full training set")
                final_model.fit(X_train, y_train)

            # Infer signature for logging
            signature = infer_signature(X_train, final_model.predict(X_train))

            # Log final model
            try:
                mlflow.sklearn.log_model(final_model,
                                         name="model",
                                         signature=signature,
                                         registered_model_name=self.model_name)
                self.logger.info("Final model logged to MLflow successfully.")
            except Exception as e:
                self.logger.warning(f"Could not log model to MLflow: {e}")

            # --- Evaluate on test set if provided ---
            if X_test is not None and y_test is not None:
                test_metrics = self._run_test_evaluation(final_model, X_test, y_test)
                self.results.update(test_metrics)

            # Save results locally
            if self.save_path:
                self.save_results()

        return self.results


    def _run_test_evaluation(self, model, X_test, y_test):
        self.logger.info("Running test set evaluation")
        y_pred = model.predict(X_test)
        test_metrics = self.evaluator.evaluate(y_test, y_pred)
        for k, v in test_metrics.items():
            mlflow.log_metric(f"test_{k}", v)
        self.logger.info(f"Test set metrics: {test_metrics}")

        # Generate and log visualisations
        self.logger.info("Generating test set visualisations")
        self._generate_visualisations(y_test, y_pred)

        return test_metrics


    def _run_cross_validation(self, X, y):
        self.logger.info(f"Running {self.cv_folds}-fold cross-validation "
                         f"(stratified={self.cv_stratified})")

        # Create folds
        if self.cv_stratified:
            self.logger.info(f"Using StratifiedKFold for cross-validation with {self.cv_folds} folds, shuffle={self.cv_shuffle}, and random_state={self.cv_random_state}")
            splitter = StratifiedKFold(n_splits=self.cv_folds, shuffle=self.cv_shuffle, random_state=self.cv_random_state)
        else:
            self.logger.info(
                f"Using KFold for cross-validation with {self.cv_folds} folds, shuffle={self.cv_shuffle}, and random_state={self.cv_random_state}")
            splitter = KFold(n_splits=self.cv_folds, shuffle=self.cv_shuffle, random_state=self.cv_random_state)

        fold_metrics = {metric: [] for metric in self.metrics}
        fold_index = 1
        X = X.to_numpy() if hasattr(X, "to_numpy") else X
        y = y.to_numpy() if hasattr(y, "to_numpy") else y

        for train_idx, val_idx in splitter.split(X, y):
            self.logger.info(f"Fold {fold_index}/{self.cv_folds}")

            X_train_fold, X_val_fold = X[train_idx], X[val_idx]
            y_train_fold, y_val_fold = y[train_idx], y[val_idx]

            # --- Apply sampler inside fold ---
            if self.sampler is not None:
                self.logger.info(f"Applying sampler '{self.sampler_name}' to training fold")
                X_train_fold, y_train_fold = self.sampler.fit_resample(X_train_fold, y_train_fold)

            # Recreate a fresh model each fold
            model = ModelFactory.get_model(self.model_name, **self.model_params)

            # refactor this later to make it safer - we should check if the model supports sample weights using hasattr or similar
            # Compute sample weights for this fold
            if self.calculate_sample_weights:
                fold_weights = self._compute_sample_weights(y_train_fold)
                fit_kwargs = {}
                if fold_weights is not None:
                    fit_kwargs['sample_weight'] = fold_weights
                self.logger.info(f"Fitting model with sample weights for fold {fold_index}")
                model.fit(X_train_fold, y_train_fold, **fit_kwargs)
            else:
                self.logger.info(f"Fitting model without sample weights for fold {fold_index}")
                model.fit(X_train_fold, y_train_fold)



            #model.fit(X_train_fold, y_train_fold)
            #model.fit(X_train_fold, y_train_fold, **fit_kwargs)

            y_pred = model.predict(X_val_fold)
            current_res = self.evaluator.evaluate(y_val_fold, y_pred)

            # Store metrics
            for m in self.metrics:
                fold_metrics[m].append(current_res[m])
                mlflow.log_metric(f"fold_{fold_index}_{m}", current_res[m])

            fold_index += 1
            # Generate and log visualisations
            self.logger.info(f"Generating visualisations for fold {fold_index - 1}")
            self._generate_visualisations(y_val_fold, y_pred, fold=fold_index - 1)

        # Compute average CV metrics
        averaged_metrics = {m: float(sum(vals) / len(vals)) for m, vals in fold_metrics.items()}
        std_metrics = {f"{m}_std": float(np.std(vals)) for m, vals in fold_metrics.items()}

        # Log averaged metrics to MLflow
        for m, avg in averaged_metrics.items():
            mlflow.log_metric(f"cv_mean_{m}", avg)
        # log std metrics to MLflow
        for m_std, std in std_metrics.items():
            mlflow.log_metric(f'cv_{m_std}', std)


        self.results = {**averaged_metrics, **std_metrics}
        self.logger.info(f"Cross-validation results: {self.results}")

        return self.results

    def save_results(self):
        os.makedirs(self.save_path, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(self.save_path, f"{self.name}_{timestamp}.json")
        try:
            with open(file_path, "w") as f:
                json.dump(
                    {
                        "experiments": self.name,
                        "model": self.model_name,
                        "metrics": self.results,
                        "params": self.model_params,
                        "timestamp": timestamp,
                    }, f, indent=4,
                )
                self.logger.info(f"Saved results locally to {file_path}")
        except Exception as e:
            self.logger.warning(f"Could not save experiment {self.name} results to {file_path}: {e}")

    def _generate_visualisations(self, y_true, y_pred, fold=0):

        if not self.visualisations:
            self.logger.info("No visualisations configured, skipping.")
            return

        self.logger.info("Generating visualisations")
        self.logger.info(f'Target encoder available: {self.target_encoder is not None}')
        for viz_cfg in self.visualisations:
            title_suffix = f"_fold_{fold}" if fold > 0 else ""

            viz_name = viz_cfg.get("name")
            self.logger.info(f'Visualisation to create: {viz_name}')
            viz_kwargs = viz_cfg.get("kwargs", {})
            viz_kwargs.update({
                "y_true": y_true,
                "y_pred": y_pred,
                "target_encoder": self.target_encoder,
            })
            try:
                viz = VisualisationFactory.get_visualisation(viz_name, **viz_kwargs)
                if viz:
                    self.logger.info(f"Creating visualisation: {viz_name}")
                    fig = viz.plot(None)  # 'data' param not needed here
                    # Save locally
                    if self.save_path:
                        os.makedirs(self.save_path, exist_ok=True)
                        filepath = os.path.join(self.save_path, f"{self.name}_{viz_name}{title_suffix}.png")
                        viz.save(fig, filepath)
                        self.logger.info(f"Saved visualisation '{viz_name}{title_suffix}' to {filepath}")
                    # Log to MLflow
                        try:
                            mlflow.log_artifact(filepath)
                        except Exception as e:
                            self.logger.warning(f"Could not log visualisation '{viz_name}' to MLflow: {e}")
            except Exception as e:
                self.logger.warning(f"Could not create visualisation '{viz_name}': {e}")

    def     _log_mlflow_params(self):
        """Log all parameters and preprocessing metadata to MLflow."""
        # --- Basic experiment info ---
        self.logger.debug("Logging experiment parameters to MLflow")
        mlflow.log_param("model", self.model_name)
        mlflow.log_param("evaluator", self.evaluator_name)
        mlflow.log_param("description", self.description)

        # --- Model params ---
        if self.model_params:
            self.logger.debug(f"Logging model parameters: {self.model_params}")
            mlflow.log_params(self.model_params)

        # --- Vectorizer params ---
        if self.vectorizer_name:
            self.logger.debug(f"Logging vectorizer parameters: {self.vectorizer_name}, {self.vectorizer_params}")
            mlflow.log_param("vectorizer_name", self.vectorizer_name)
            mlflow.log_param("vectorizer_field", self.vectorizer_field)
            for k, v in self.vectorizer_params.items():
                mlflow.log_param(f"vectorizer_{k}", v)

        # --- Cross-validation params ---
        self.logger.debug("Logging cross-validation parameters")
        mlflow.log_param("cv_enabled", self.cv_enabled)
        mlflow.log_param("cv_folds", self.cv_folds)
        mlflow.log_param("cv_stratified", self.cv_stratified)
        mlflow.log_param("cv_shuffle", self.cv_shuffle)
        mlflow.log_param("cv_random_state", self.cv_random_state)



        # --- Preprocessing metadata ---
        if self.preprocessing_metadata:
            # Global preprocessing
            self.logger.debug("Logging preprocessing metadata to MLflow")
            for step in self.preprocessing_metadata.get("global_preprocessing", []):
                name = step["name"]
                params = step.get("params", {})
                for k, v in params.items():
                    mlflow.log_param(f"global_pre_{name}_{k}", v)

            # --- Data cleanup steps ---
            self.logger.debug("Logging data cleanup steps to MLflow")
            for step in self.preprocessing_metadata.get("data_cleanup", []):
                name = step.get("name", "unknown")
                params = step.get("params", {})

                for k, v in params.items():
                    self.logger.info(f"Logging cleanup param: {name}_{k} = {v}")
                    # Safely stringify complex values
                    if isinstance(v, (list, dict)):
                        v = json.dumps(v)
                    mlflow.log_param(f"cleanup_{name}_{k}", v)

            # Experiment preprocessing
            self.logger.debug("Logging experiment-specific preprocessing metadata to MLflow")
            for step in self.preprocessing_metadata.get("experiment_preprocessing", []):
                name = step["name"]
                params = step.get("params", {})
                for k, v in params.items():
                    mlflow.log_param(f"exp_pre_{name}_{k}", v)

            # Roblox extraction
            self.logger.debug("Logging Roblox extraction metadata to MLflow")
            for k, v in self.preprocessing_metadata.get("roblox_extraction", {}).items():
                mlflow.log_param(f"roblox_{k}", v)

            # Data split
            self.logger.debug("Logging data split metadata to MLflow")
            for k, v in self.preprocessing_metadata.get("split_data", {}).items():
                mlflow.log_param(f"split_{k}", v)

            # Visualisations
            if self.visualisations:
                self.logger.debug("Logging visualisation metadata to MLflow")
                for viz in self.visualisations:
                    viz_name = viz.get("name")
                    viz_kwargs = viz.get("kwargs", {})
                    mlflow.log_param(f"viz_{viz_name}", json.dumps(viz_kwargs))

        self.logger.info("All experiment parameters and preprocessing metadata logged to MLflow.")

    def _map_sampler_strategy_to_numeric(self):
        """
        Converts YAML string-based sampling_strategy labels (e.g. 'All')
        into their numeric-encoded equivalents using target_encoder.
        """

        if not self.sampler_params or not self.target_encoder:
            self.logger.info("No sampler params or target_encoder available – skipping mapping.")
            return

        # Case 1: Direct strategy on the sampler
        self._convert_strategy_dict(self.sampler_params)

        # Case 2: Composite sampler steps
        if "steps" in self.sampler_params:
            for step in self.sampler_params["steps"]:
                if "params" in step:
                    self._convert_strategy_dict(step["params"])

    def _convert_strategy_dict(self, params: dict):
        """Convert sampling_strategy labels to numeric values in-place."""

        orig_strategy = params.get("sampling_strategy")

        if not orig_strategy or not isinstance(orig_strategy, dict):
            return

        self.logger.info(f"Original sampler strategy: {orig_strategy}")

        numeric_strategy = {}

        # Get known class labels from encoder for validation
        valid_labels = set(self.target_encoder.classes_)

        for label_str, n_samples in orig_strategy.items():

            if label_str not in valid_labels:
                self.logger.warning(
                    f"Label '{label_str}' not found in target encoder classes: {valid_labels}"
                )
                continue

            numeric_code = int(self.target_encoder.transform([label_str])[0])
            numeric_strategy[numeric_code] = n_samples

            self.logger.info(
                f"Mapped sampler label '{label_str}' → {numeric_code}"
            )

        params["sampling_strategy"] = numeric_strategy
        self.logger.info(f"Updated sampler strategy: {numeric_strategy}")

    def _compute_sample_weights(self, y):
        counter = Counter(y)
        classes = list(counter.keys())
        counts = np.array(list(counter.values()))

        total_samples = len(y)
        num_classes = len(classes)

        class_weights = {
            c: total_samples / (num_classes * count)
            for c, count in counter.items()
        }

        sample_weights = np.array([class_weights[label] for label in y])
        return sample_weights