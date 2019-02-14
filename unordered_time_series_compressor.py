class UnorderedTimeSeriesCompressor:
    def __init__(self, data_size, base_compressor):
        # This is prototype code and doesn't validate arguments
        self.data_size = data_size * 4
        self.base_compressor = base_compressor
        self._multithreading_threshold = 10000
        self._chunks = {}
        
    def _compress_chunk(self, time_series_chunk):                
        if len(time_series_chunk) <= self._multithreading_threshold:
            ordered_time_series_chunk = OrderedDict(sorted(time_series_chunk.items(), key=lambda t: t[0]))
            size = len(self._chunks)
            self._chunks[size] = self.base_compressor._run(ordered_time_series_chunk)
        else:
            thread_size = (len(time_series) + self._multithreading_threshold - 1) // self._multithreading_threshold
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
                threads[i] = threading.Thread(target=_compress_chunk, args=(results, chunk_collections[i]))
                threads[i].start()
                threads[i].join()
                
    def _final_compress(self):            
        merged_chunks = {}
            
        if len(self._chunks) == 0:
            return merged_chunks
        
        merged_chunks.update(self._chunks)
                
        iteration = 1
        
        while len(merged_chunks) > 1:
            intermediate_chunks = {}
            intermediate_index = 0
            
            merged_chunks_times = iter(merged_chunks.keys())

            next_merged_chunk_time = next(merged_chunks_times, None)
            next_next_merged_chunk_time = next(merged_chunks_times, None)
            
            is_even = False
            
            while next_merged_chunk_time is not None and next_next_merged_chunk_time is not None:
                merged_chunk = self.__merge_sorted(merged_chunks[next_merged_chunk_time], merged_chunks[next_next_merged_chunk_time])
                intermediate_chunks[intermediate_index] = (merged_chunk)
                intermediate_index += 1
                
                next_merged_chunk_time = next(merged_chunks_times, None)
                next_next_merged_chunk_time = next(merged_chunks_times, None)
            
            if next_merged_chunk_time is not None:        
                intermediate_chunks[intermediate_index] = merged_chunks[next_merged_chunk_time]
                
            merged_chunks.clear()
            merged_chunks.update(intermediate_chunks)
                        
            iteration += 1
        
        compressed_data = self.base_compressor._run(merged_chunks[0])
        
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