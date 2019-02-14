import base_compression

class SwingingDoorCompression(BaseCompression):
    
    def __init__(self, data_size):
        # This is prototype code and doesn't validate arguments
        super().__init__(data_size, data_size / 10, 30000, 100)
        
    def _run(self, time_series):
        return super()._run_common_with_binary_search(time_series, self._run_swinging_door_internal)
    
    def _run_swinging_door_internal(self, results, time_series, swinging_door_deviation, swinging_door_window_size = +inf):
        first_value_encountered = False

        swinging_door = {}

        swinging_door_window_counter = 0

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

                swinging_door[snapshot_time] = snapshot_value
                first_value_encountered = True
                continue

            time_delta_ticks = 1000000 #we should use datetime ticks, but python perf suffer 
                                        #((time - snapshot_time).total_seconds() * 1000)
            curr_slope = (value - snapshot_value) / time_delta_ticks

            curr_slope_max = (value + swinging_door_deviation - snapshot_value) / time_delta_ticks
            curr_slope_min = (value - swinging_door_deviation - snapshot_value) / time_delta_ticks

            slope_max = min(slope_max, curr_slope_max)
            slope_min = max(slope_min, curr_slope_min)

            if curr_slope < slope_min or curr_slope > slope_max:
                swinging_door_window_counter = 0
            else:
                swinging_door_window_counter = (swinging_door_window_counter + 1) % swinging_door_window_size

            if swinging_door_window_counter == 0:
                if last_time not in swinging_door:
                    swinging_door[last_time] =  last_value
                if time not in swinging_door:
                    swinging_door[time] = value

                snapshot_time = time
                snapshot_value = value

                last_time_delta_ticks = 1000000 #we should use datetime ticks, but python perf suffer 
                                                #((time - last_time).total_seconds() * 1000)
                slope_max = (value + swinging_door_deviation - last_value) / last_time_delta_ticks
                slope_min = (value - swinging_door_deviation - last_value) / last_time_delta_ticks

            last_time = time
            last_value = value
            
        if last_time not in swinging_door:
            swinging_door[last_time] = last_value
            
        results.update(swinging_door)