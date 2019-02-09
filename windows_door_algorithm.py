import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas.plotting import lag_plot
import time as tm
from datetime import timedelta
from datetime import datetime
import math
from math import inf
import threading
from threading import Thread

class WindowsDoorCompression:
    
    def __init__(self, data_size):
        # This is prototype code and doesn't validate arguments
        self._data_size = data_size
        self._tolerance = data_size / 10
        self._multithreading_threshold = 50000
        
    def _run_exception(self, time_series):
        t0 = tm.time()

        minVal = min(list(time_series.values()))
        maxVal = max(list(time_series.values()))
        
        exception = {}
        exception.update(time_series)
                
        prev_exception = {}
        prev_exception.update(time_series)
        
        exception_deviation = 1
        
        prev_exception_deviation = exception_deviation

        start = 1
        end = maxVal - minVal
        
        if len(exception) > self._multithreading_threshold:
            while True:                
                exception = self._run_exception_multithread(exception, exception_deviation)
        
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

        print("{} {} {} {} {} {}".format("prev_exception data size: ", len(prev_exception), " start ", start, " end ", end))

        truncated_time_series =  self._binary_search(prev_exception, start, end, self._run_exception_internal)
             
        t1 = tm.time()

        print("{} {}".format(t0, " seconds time elapsed in running exception."))
        print("{} {}".format(t1, " seconds time elapsed in running exception."))

        print("{} {}".format((t1 - t0), " seconds time elapsed in running exception."))
        print("{} {}".format((len(time_series) - len(truncated_time_series)), "number of data points discarded as part of exception window"))
        print("{} {}".format(len(truncated_time_series), "data points remaining"))
        
        return truncated_time_series
    
    def _run_compression(self, time_series):
        t0 = tm.time()

        minVal = min(list(time_series.values()))
        maxVal = max(list(time_series.values()))
        
        compression = {}
        compression.update(time_series)
                
        prev_compression = {}
        prev_compression.update(time_series)
        
        compression_deviation = 1
        
        prev_compression_deviation = compression_deviation

        start = 1
        end = maxVal - minVal
        
        if len(compression) > self._multithreading_threshold:
            while True:                
                compression = self._run_multithread(compression, compression_deviation, self._run_compression_internal)
        
                if len(prev_compression) >= self._data_size and len(compression) <= self._data_size:
                    start = 1
                    end = max(compression_deviation, prev_compression_deviation)
                    break

                if len(compression) >= self._data_size:
                    prev_compression_deviation = compression_deviation
                    compression_deviation *= 2
                    prev_compression = compression
                else:
                    compression_deviation /= 2
                    compression = compression_deviation

        print("{} {} {} {} {} {}".format("prev_compression data size: ", len(prev_compression), " start ", start, " end ", end))

        truncated_time_series =  self._binary_search(prev_compression, start, end, self._run_compression_internal)
             
        t1 = tm.time()

        print("{} {}".format(t0, " seconds time elapsed in running compression."))
        print("{} {}".format(t1, " seconds time elapsed in running compression."))

        print("{} {}".format((t1 - t0), " seconds time elapsed in running compression."))
        print("{} {}".format((len(time_series) - len(truncated_time_series)), "number of data points discarded as part of compression window"))
        print("{} {}".format(len(truncated_time_series), "data points remaining"))
        
        return truncated_time_series
    
    def _binary_search(self, time_series, start, end, run_internal):
        truncated_time_series = {}
        
        optimal_found = False
        
        next_best_time_series = {}
                
        while start + 1e-5 <= end:
            mid = start + (end - start) / 2
            
            new_truncated_time_series = {}
                        
            print("{} {} {} {} {} {}".format("before time_series data size: ", len(time_series), " start ", start, " end ", end))

            run_internal(new_truncated_time_series, time_series, mid)
            
            print("{} {} {} {} {} {}".format("after new_truncated_time_series data size: ", len(new_truncated_time_series), " start ", start, " end ", end))
                        
            if len(new_truncated_time_series) < self._data_size - self._tolerance:
                end = mid
                if abs(len(new_truncated_time_series) - (self._data_size - self._tolerance)) < abs(len(next_best_time_series) - (self._data_size - self._tolerance)):
                    next_best_time_series = new_truncated_time_series
            elif len(new_truncated_time_series) > self._data_size + self._tolerance:
                start = mid
                if abs(len(new_truncated_time_series) - (self._data_size + self._tolerance)) < abs(len(next_best_time_series) - (self._data_size + self._tolerance)):
                    next_best_time_series = new_truncated_time_series
            else:
                optimal_found = True
                truncated_time_series = new_truncated_time_series
                break
        
        if not optimal_found:
            truncated_time_series = next_best_time_series
        
        return truncated_time_series
        
    def _run_multithread(self, time_series, exception_deviation, run_internal, exception_window_size = +inf):
        thread_size = (len(time_series) + self._multithreading_threshold - 1) // self._multithreading_threshold
        threads = [None] * thread_size
        
        results = {}
        
        time_series_slice_collections = {}
        
        for i in range(thread_size):
            time_series_slice = {}

            count = 0
            for time, value in time_series.items():
                if count >= self._multithreading_threshold:
                    break

                time_series_slice[time] = value
                count += 1

            threads[i] = threading.Thread(target=run_internal, args=(results, time_series_slice, exception_deviation))
            threads[i].start()
            threads[i].join()
                    
        return results
        
    def _run_exception_internal(self, results, time_series, exception_deviation, exception_window_size = +inf):

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
                    exception[last_time] = last_value
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

        results.update(exception)
        
    def _run_compression_internal(self, results, time_series, compression_deviation, compression_window_size = +inf):
        first_value_encountered = False

        compression = {}

        compression_window_counter = 0

        last_time = next(iter(time_series.keys()))
        last_value = next(iter(time_series.values()))

        slope_max = +inf
        slope_min = -inf

        for time, value in time_series.items():
            if math.isnan(value):
                continue
            if not first_value_encountered:
                snapshot_time = time
                snapshot_value = value

                compression[snapshot_time] = snapshot_value
                first_value_encountered = True
                continue

            curr_slope = (value - snapshot_value) / ((time - snapshot_time).total_seconds() / 1000.0)

            curr_slope_max = (value + compression_deviation - snapshot_value) / ((time - snapshot_time).total_seconds() * 1000)
            curr_slope_min = (value - compression_deviation - snapshot_value) / ((time - snapshot_time).total_seconds() * 1000)

            slope_max = min(slope_max, curr_slope_max)
            slope_min = max(slope_min, curr_slope_min)

            print("{} {} {}".format(curr_slope, slope_min, slope_max))

            if curr_slope < slope_min or curr_slope > slope_max:
                compression_window_counter = 0
            else:
                compression_window_counter = (compression_window_counter + 1) % compression_window_size

            if compression_window_counter == 0:
                if last_time not in compression:
                    compression[last_time] =  last_value
                if time not in compression:
                    compression[time] = value

                snapshot_time = time
                snapshot_value = value

                slope_max = (value + compression_deviation - last_value) / ((time - last_time).total_seconds() * 1000)
                slope_min = (value - compression_deviation - last_value) / ((time - last_time).total_seconds() * 1000)

            last_time = time
            last_value = value
            
        if last_time not in compression:
            compression[last_time] = last_value
            
        results.update(compression)
        
        print("{}".format(len(results)))