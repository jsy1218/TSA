import base_compression

class BoxCarCompression(BaseCompression):
    
    def __init__(self, data_size):
        # This is prototype code and doesn't validate arguments
        super().__init__(data_size, data_size / 10, 1000000, 100)
        
    def _run(self, time_series):
        return super()._run_common_with_binary_search(time_series, self._run_box_car_internal)
    
    def _run_box_car_internal(self, results, time_series, box_car_deviation, box_car_window_size = +inf):
        box_car = {}

        box_car_window_counter = 0

        first_value_encountered = False

        last_time = next(iter(time_series.keys()))
        last_value = next(iter(time_series.values()))

        for time, value in time_series.items():
            if math.isnan(value):
                continue
            if not first_value_encountered:
                snapshot_time = time
                snapshot_value = value

                box_car[snapshot_time] = snapshot_value
                first_value_encountered = True
                continue

            if abs(snapshot_value - value) > box_car_deviation:
                if last_time not in box_car:
                    box_car[last_time] = last_value
                if time not in box_car:
                    box_car[time] = value
                box_car_window_counter = 0
                
                snapshot_time = time
                snapshot_value = value
            else:
                box_car_window_counter = (box_car_window_counter + 1) % box_car_window_size

                if box_car_window_counter == 0:
                    if time_series.keys()[index] not in box_car:
                        box_car[time] = value

            if box_car_window_counter == 0:
                snapshot_time = time
                snapshot_value = value

            last_time = time
            last_value = value

        if last_time not in box_car:
            box_car[last_time] = last_value

        results.update(box_car)