from enum import Enum
import base_compression

class Algorithm(Enum):
    BoxCar = 1
    SwingingDoor = 2
    Hybrid = 3

class UnorderedTimeSeriesCompressor:
    def __init__(self, data_size, algorithm):
        # This is prototype code and doesn't validate arguments
        
        if algorithm == Algorithm.BoxCar:
            self.base_compressor = BoxCarCompression(data_size / 2)
            self.final_compressor = BoxCarCompression(data_size)
        elif algorithm == Algorithm.SwingingDoor:
            self.base_compressor = SwingingDoorCompression(data_size / 2)
            self.final_compressor = SwingingDoorCompression(data_size)
        else:
            self.base_compressor = HybridCompression(data_size / 2)
            self.final_compressor = HybridCompression(data_size)
            
        self._multithreading_threshold = 10000
        self._chunks = {}
        
    def _compress_chunk(self, time_series_chunk, depth = 0):               
        if len(time_series_chunk) <= self._multithreading_threshold:
            ordered_time_series_chunk = OrderedDict(sorted(time_series_chunk.items(), key=lambda t: t[0]))
            size = len(self._chunks)
            self._chunks[size] = self.base_compressor._run(ordered_time_series_chunk)
        else:
            thread_size = (len(time_series_chunk) + self._multithreading_threshold - 1) // self._multithreading_threshold
            threads = [None] * thread_size
            
            single_chunk = {}
            chunk_collections = {}
            
            count = 0
            for time, value in time_series_chunk.items():
                if count % self._multithreading_threshold == 0:
                    single_chunk = {}
                    chunk_collections[count // self._multithreading_threshold] = single_chunk

                single_chunk[time] = value
                count += 1
            
            for i in range(thread_size):
                
                threads[i] = threading.Thread(target=self._compress_chunk, args=(chunk_collections[i], depth + 1))
                threads[i].start()
                threads[i].join()
                
    def _final_compress(self):            
        merged_chunks = {}
            
        if len(self._chunks) == 0:
            return merged_chunks
                        
        index = 0
                
        while index in self._chunks and index + 1 in self._chunks:
            merged_chunk = self.__merge_sorted(self._chunks[index], self._chunks[index + 1])
            merged_chunks.update(merged_chunk)

            index += 2

        if index in self._chunks:        
            merged_chunks.update(self._chunks[index])
        
        self._chunks.clear()
                                        
        compressed_data = self.final_compressor._run(merged_chunks)
        
        return compressed_data
    
    def __merge_sorted(self, chunk_1, chunk_2):
        merged_chunk = {}
        
        chunk_1_times = iter(chunk_1.keys())
        chunk_2_times = iter(chunk_2.keys())
                
        next_chunk_1 = next(chunk_1_times, None)
        next_chunk_2 = next(chunk_2_times, None)
        
        while next_chunk_1 is not None and next_chunk_2 is not None:
            size = len(merged_chunk)
            
            if next_chunk_1 <= next_chunk_2:
                merged_chunk[next_chunk_1] = chunk_1[next_chunk_1]
                next_chunk_1 = next(chunk_1_times, None)
            else:
                merged_chunk[next_chunk_2] = chunk_2[next_chunk_2]
                next_chunk_2 = next(chunk_2_times, None)
            
        while next_chunk_1 is not None:
            size = len(merged_chunk)
            
            merged_chunk[next_chunk_1] = chunk_1[next_chunk_1]
            
            next_chunk_1 = next(chunk_1_times, None)
            
        while next_chunk_2 is not None:
            size = len(merged_chunk)
            
            merged_chunk[next_chunk_2] = chunk_2[next_chunk_2]
            
            next_chunk_2 = next(chunk_2_times, None)
            
        return merged_chunk