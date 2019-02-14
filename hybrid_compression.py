import base_compression

class HybridCompression(BoxCarCompression, SwingingDoorCompression):
    
    def __init__(self, data_size):
        # This is prototype code and doesn't validate arguments
        super(BoxCarCompression, self).__init__(data_size)
        super(SwingingDoorCompression, self).__init__(data_size, data_size / 10, 30000, 100)
        
    def _run(self, time_series):
        prev_refined_time_series, start, end = super()._run_common(time_series, super()._run_box_car_internal)
        return super()._binary_search(prev_refined_time_series, start, end, super()._run_swinging_door_internal)