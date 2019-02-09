import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas.plotting import lag_plot
import time as tm
from datetime import timedelta
from datetime import datetime
from multiprocessing.pool import Pool
import math
from math import inf

class WindowsDoorCompression:
    
    def __init__(self, data_size):
        # This is prototype code and doesn't validate arguments
        self._data_size = data_size
        self._tolerance = data_size / 10
        self._multithreading_threshold = 10000
        
    def _run_exception(self, time_series):
        t0 = tm.time()

        exception = {}
        exception.update(time_series)
        
        prev_exception = {}
        prev_exception.update(time_series)
        
        exception_deviation = np.std(list(time_series.values())) / self._data_size
        
        prev_exception_deviation = exception_deviation

        start = -inf
        end = +inf

        while True:                
            if len(exception) <= self._multithreading_threshold:
                exception = (self.__run_exception_internal(exception, exception_deviation))
            else:
                multiple_results = self.__run_exception_multithread(exception, exception_deviation)

                exception = {}
                
                for res in multiple_results:
                    exception.update(res.get(timeout=10))
        
            if len(prev_exception) >= self._data_size and len(exception) <= self._data_size:
                start = 1
                end = max(exception_deviation, prev_exception_deviation)
                break

            if len(exception) >= self._data_size:
                prev_exception_deviation = exception_deviation
                exception_deviation *= 2
                prev_exception = exception
            else:
                exception_deviation /= 2
                exception = prev_exception

        truncated_time_series =  self.__binary_search(prev_exception, start, end)
             
        t1 = tm.time()

        print("{} {}".format(t0, " seconds time elapsed in running exception."))
        print("{} {}".format(t1, " seconds time elapsed in running exception."))

        print("{} {}".format((t1 - t0), " seconds time elapsed in running exception."))
        print("{} {}".format((len(time_series) - len(truncated_time_series)), "number of data points discarded as part of exception window"))
        print("{} {}".format(len(truncated_time_series), "data points remaining"))
        
        return truncated_time_series
        
    def __binary_search(self, time_series, start, end):
        truncated_time_series = {}
                
        while start + 1e-5 <= end:
            mid = start + (end - start) / 2
            
            new_truncated_time_series = self.__run_exception_internal(time_series, mid)
            
            if len(new_truncated_time_series) < self._data_size - self._tolerance:
                end = mid - 1
            elif len(new_truncated_time_series) > self._data_size + self._tolerance:
                start = mid + 1
            else:
                truncated_time_series = new_truncated_time_series
                break
        
        return truncated_time_series
        
    def __run_exception_multithread(self, time_series, exception_deviation, exception_window_size = +inf):
        pool_size = (len(time_series) + self._multithreading_threshold - 1) // self._multithreading_threshold
        pool = Pool(processes=pool_size)
        multiple_results = []
        
        time_series_slice_collections = {}
        
        for i in range(pool_size):
            time_series_slice = {}

            count = 0
            for time, value in time_series.items():
                if count >= self._multithreading_threshold:
                    break

                time_series_slice[time] = value
                count += 1

            time_series_slice_collections[i] = time_series_slice

        multiple_results = [pool.apply_async(self.__run_exception_internal, (time_series_slice_collections[i], exception_deviation)) for i in range(pool_size)]
                    
        return multiple_results
        
    def __run_exception_internal(self, time_series, exception_deviation, exception_window_size = +inf):

        exception = {}

        exception_window_counter = 0

        first_value_encountered = False

        last_time = next(iter(time_series.keys()))
        last_value = next(iter(time_series.values()))

        for time, value in time_series.items():
            if math.isnan(value):
                continue
            if not first_value_encountered:
                snapshot_time = time
                snapshot_value = value

                exception[snapshot_time] = snapshot_value
                first_value_encountered = True
                continue
                
            if abs(snapshot_value - value) > exception_deviation:
                if last_time not in exception:
                    exception[last_time] =  last_value
                if time not in exception:
                    exception[time] = value
                exception_window_counter = 0
            else:
                exception_window_counter = (exception_window_counter + 1) % exception_window_size

                if exception_window_counter == 0:
                    if time_series.keys()[index] not in exception:
                        exception[time] = value

            if exception_window_counter == 0:
                snapshot_time = time
                snapshot_value = value

            last_time = time
            last_value = value

        if last_time not in exception:
            exception[last_time] = last_value
        
        return exception