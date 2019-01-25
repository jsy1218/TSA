import bisect
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


class StreamingLeastSquaresDetector:
    def __init__(self, lag):
        # This is prototype code and doesn't validate arguments
        self._lag = lag
        self._coef_matrix = self._compute_coef_matrix(lag)

    # Given the window size w, pre-compute X(X^T X)^{-1}X^T - I,
    # in which X = [0   1
    #               1   1
    #               2   1
    #               ...
    #             (w-1) 1]
    def _compute_coef_matrix(self, w):
        from numpy import array, arange, ones, linalg, eye
        X = array([arange(w), ones([w])]).transpose()
        return X @ linalg.inv(X.transpose() @ X) @ X.transpose() - eye(w)

    def detect_anomalies(self, data, visualize=True):
        if type(data) != pd.Series:
            raise ValueError('data must be of the pandas Series type')
        if self._lag < 3:
            raise ValueError('lag needs to be at least 3.')

        data = data.fillna(method='pad')  # fill NANs with 0 to make the series contiguous

        coefs = self._compute_coef_matrix(self._lag)

        values = data.values
        num_windows = len(values) - self._lag + 1
        windows = np.vstack(values[ix:ix + num_windows] for ix in range(self._lag))
        residuals = np.linalg.norm(coefs @ windows, axis=0)

        windows = [(ix, residuals[ix]) for ix in range(num_windows)]

        anomaly_partitions = self._partition_anomalies(windows)

        timestamps = data.index
        anomalies = []
        for anomaly_partition in enumerate(anomaly_partitions):
                start_index = anomaly_partition[1][0]
                end_index = anomaly_partition[1][1]
                score = anomaly_partition[1][2]
                anomalies.append((str(timestamps[start_index]), str(timestamps[end_index]), score))
        anomalies = pd.DataFrame(anomalies, columns=['start', 'end', 'score'])

        if visualize:
            from IPython.display import display
            display(anomalies)

            data.plot(title='lag: {0}'
                    .format(self._lag))
            for anomaly in anomalies.values:
                plt.axvspan(anomaly[0], anomaly[1], color=plt.cm.jet(0.65), alpha=0.5)

        return anomalies

    # TODO ktran: improve the algorithm
    def _partition_anomalies(self, windows):
        """
        :param windows: windows, residual score in the original order
        :return: partitioned anomalies
        """
        residuals = [j for i,j in windows]

        mean = np.mean(residuals)
        standard_deviation = np.std(residuals)

        temp_partitions = []

        for ix in range(len(residuals)):
            if (residuals[ix] >= mean + standard_deviation * 3 or residuals[ix] <= mean - standard_deviation * 3):
                temp_partitions.append((ix, ix + self._lag - 1, residuals[ix]))

        result_partitions = []

        for ix in range(1, len(temp_partitions)):
            if (self._has_overlap(temp_partitions[ix - 1], temp_partitions[ix])):
                start = min(temp_partitions[ix - 1][0], temp_partitions[ix][0])
                end = max(temp_partitions[ix - 1][1], temp_partitions[ix][1])
                residual = max(temp_partitions[ix - 1][2], temp_partitions[ix][2])

                result_partitions.append((start, end, residual))
                ix = ix - 1
        
        return result_partitions

    def _has_overlap(self, partition1, partition2):
        if (partition1[1] >= partition2[0] and partition1[1] <= partition2[1]):
            return True
        if (partition1[0] >= partition2[0] and partition1[0] <= partition2[1]):
            return True
        return False