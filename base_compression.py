import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
import math
from math import inf
from math import sqrt
import threading
from threading import Thread

class BaseCompression:
    def __init__(self, data_size, tolerance, multithreading_threshold, max_iteration):
        # This is prototype code and doesn't validate arguments
        self._data_size = data_size
        self._tolerance = tolerance
        self._multithreading_threshold = multithreading_threshold
        self._max_iteration = max_iteration
    
    def _run(self, time_series):
        pass
    
    def __run_box_car_internal(self, results, time_series, box_car_deviation, box_car_window_size = +inf):
        pass
    
    def __run_swinging_door_internal(self, results, time_series, swinging_door_deviation, swinging_door_window_size = +inf):
        pass
    
    def _run_common(self, time_series, run_common_method):
        time_series_list = list(time_series.values())
        
        refined_time_series = {}
        refined_time_series.update(time_series)
                
        prev_refined_time_series = {}
        prev_refined_time_series.update(time_series)
                
        index = 0
        prev_val = 0
        
        minVal = time_series_list[0]
        maxVal = time_series_list[0]
        
        mean_sum = time_series_list[0]
        squared_sum = time_series_list[0] * time_series_list[0]
        
        for index in range(1, len(time_series_list)):
            minVal = min(minVal, time_series_list[index])
            maxVal = max(maxVal, time_series_list[index])
            mean_sum += time_series_list[index]
            squared_sum += time_series_list[index] * time_series_list[index]
        
        mean = mean_sum / len(time_series_list)
        time_series_deviation = sqrt(squared_sum / len(time_series_list) - mean * mean) / 10
        
        print("{} {}".format("Initial deviation:", time_series_deviation))

        prev_time_series_deviation = time_series_deviation

        start = 1
        end = maxVal - minVal
        
        if len(refined_time_series) > self._multithreading_threshold:
            iteration = 1
            while iteration <= self._max_iteration * 10:                
                refined_time_series = self._run_multithread(refined_time_series, time_series_deviation, run_common_method)
        
                if len(prev_refined_time_series) >= self._data_size and len(refined_time_series) <= self._data_size:
                    start = 1
                    end = max(time_series_deviation, prev_time_series_deviation)
                    break

                if len(refined_time_series) > self._data_size:
                    prev_time_series_deviation = time_series_deviation
                    time_series_deviation *= 2
                    prev_refined_time_series = refined_time_series
                else:
                    time_series_deviation /= 2
                    refined_time_series = prev_refined_time_series
                
                iteration += 1
                
                print("{} {} {} {}".format(iteration, " iteration: ", len(refined_time_series), " data points remaining. "))

        return prev_refined_time_series, start, end
        
    def _run_common_with_binary_search(self, time_series, run_common_method):
        prev_refined_time_series, start, end = self._run_common(time_series, run_common_method)

        truncated_time_series =  self._binary_search(prev_refined_time_series, start, end, run_common_method)
                     
        return truncated_time_series
    
    def _binary_search(self, time_series, start, end, run_internal):
        truncated_time_series = {}
        
        optimal_found = False
        
        next_best_time_series = time_series
                
        iteration = 1
        while start + 1e-5 <= end and iteration <= self._max_iteration:
            mid = start + (end - start) / 2
            
            new_truncated_time_series = {}
                        
            run_internal(new_truncated_time_series, time_series, mid)
                                    
            if len(new_truncated_time_series) < self._data_size - self._tolerance:
                end = mid
                if abs(len(new_truncated_time_series) - (self._data_size - self._tolerance)) < abs(len(next_best_time_series) - (self._data_size - self._tolerance)):
                    next_best_time_series = new_truncated_time_series
            elif len(new_truncated_time_series) > self._data_size + self._tolerance:
                start = mid
            else:
                optimal_found = True
                truncated_time_series = new_truncated_time_series
                break
            
            iteration += 1
            
        if not optimal_found:
            truncated_time_series = next_best_time_series
        
        return truncated_time_series
        
    def _run_multithread(self, time_series, time_series_deviation, run_internal, time_series_window_size = +inf):
        thread_size = (len(time_series) + self._multithreading_threshold - 1) // self._multithreading_threshold
        threads = [None] * thread_size
        
        results = {}
        
        time_series_slice_collections = {}
        time_series_slice = {}
        
        count = 0
        for time, value in time_series.items():
            if count % self._multithreading_threshold == 0:
                time_series_slice = {}
                time_series_slice_collections[count // self._multithreading_threshold] = time_series_slice

            time_series_slice[time] = value
            count += 1

        for i in range(thread_size):
            threads[i] = threading.Thread(target=run_internal, args=(results, time_series_slice_collections[i], time_series_deviation, time_series_window_size))
            threads[i].start()
            threads[i].join()
                    
        return results