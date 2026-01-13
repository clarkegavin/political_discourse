#visualisations/confusion_matrix_chart.py
from .base import Visualisation
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix
from sklearn.metrics import ConfusionMatrixDisplay

class ConfusionMatrixChart(Visualisation):
    def __init__(self, y_true, y_pred, labels=None, target_encoder = None, title: str = 'Confusion Matrix', **kwargs):
        super().__init__(title=title)
        self.y_true = y_true
        self.y_pred = y_pred
        self.labels = labels
        self.target_encoder = target_encoder
        self.kwargs = kwargs


    def plot(self, data, **kwargs):

        self.logger.info('Creating confusion matrix plot')
        display_labels = self.labels


        if hasattr(self, 'target_encoder') and self.target_encoder:
            # self.logger.info('Decoding labels using target encoder')
            # #display_labels = self.target_encoder.classes_
            # all_labels = self.target_encoder.classes_
            # present_labels = sorted(list(set(self.y_true) | set(self.y_pred)))
            # # Only keep labels that exist in encoder
            # display_labels = [lbl for lbl in all_labels if lbl in present_labels]
            # self.logger.info(f'Decoded labels: {display_labels}')
            # # log if any labels are missing
            # missing_labels = set(present_labels) - set(display_labels)
            # if missing_labels:
            #     self.logger.warning(f'Some labels are missing in target encoder: {missing_labels}')
            self.logger.info('Decoding labels using target encoder')
            y_true_decoded = self.target_encoder.inverse_transform(self.y_true)
            y_pred_decoded = self.target_encoder.inverse_transform(self.y_pred)

            all_labels = list(self.target_encoder.classes_)
            present_labels = sorted(list(set(y_true_decoded) | set(y_pred_decoded)))
            display_labels = [lbl for lbl in all_labels if lbl in present_labels]

            self.logger.info(f'Decoded labels: {display_labels}')
            missing_labels = set(present_labels) - set(display_labels)
            if missing_labels:
                self.logger.warning(f'Some labels are missing in target encoder: {missing_labels}')

            # update y_true and y_pred for plotting
            self.y_true = y_true_decoded
            self.y_pred = y_pred_decoded

        fig, ax = plt.subplots(figsize=self.kwargs.get('figsize', (10, 6)))
        disp = ConfusionMatrixDisplay.from_predictions(
            self.y_true,
            self.y_pred,
            # labels=range(len(display_labels)) if display_labels is not None else None,
            labels=None,  # leave as None if y_true/y_pred already have all classes
            display_labels=display_labels,  # <--- this is what shows on axes
            ax=ax,
            xticks_rotation=45,
            cmap=self.kwargs.get('cmap', 'Blues'),
            normalize=self.kwargs.get('normalize', None)
        )
        plt.title(self.title)
        plt.close(fig)  # Prevent display in some environments
        self.logger.info('Confusion matrix plot created')

        return fig