#ifndef DBX1000_CONFLICT_GRAPH_H
#define DBX1000_CONFLICT_GRAPH_H

class ConflictGraphGenerator {
public:
    void initialize(uint32_t num_threads,
                    uint64_t num_params_per_thread,
                    uint64_t num_params_pgpt,
                    const char * base_file_name) {
        _num_threads = num_threads;
        _num_params_per_thread = num_params_per_thread;
        _num_params_pgpt = num_params_pgpt;
        _base_file_name = new char[100];
        strcpy(_base_file_name, base_file_name);
        open_all_files();
    }

    void create_all_conflict_graphs() {
        uint64_t num_params_done_pt = 0;
        uint32_t iteration = 0;
        while(num_params_done_pt < _num_params_per_thread) {
            //specify region to read
            uint64_t start_offset = iteration * _num_params_pgpt;
            uint64_t num_records = min(start_offset + _num_params_pgpt, _num_params_per_thread) - start_offset;

            //create a file
            char * file_name = get_graph_file_name(_base_file_name, iteration);
            FILE * out_graph_file = fopen(file_name, "w");

            //create and write conflict graph
            create_conflict_graph(start_offset, num_records, out_graph_file);

            //flush and close file
            fflush(out_graph_file);
            fclose(out_graph_file);

            //move to next region
            num_params_done_pt += _num_params_pgpt;
            iteration++;
        }
    }

    void finalize() {
        close_all_files();
    }

protected:
    void open_all_files() {
        _files = new FILE * [_num_threads];
        char * file_name;
        for(uint32_t thread_id = 0; thread_id < _num_threads; thread_id++) {
            file_name = get_workload_file(_base_file_name, thread_id);
            _files[thread_id] = fopen(file_name, "r");
            if(_files[thread_id] == NULL) {
                printf("Error opening file: %s\n", file_name);
                exit(0);
            }

            delete file_name;
        }
    }

    void close_all_files() {
        for(uint32_t thread_id = 0; thread_id < _num_threads; thread_id++) {
            fclose(_files[thread_id]);
        }
    }

    virtual void create_conflict_graph(uint64_t start_offset, uint64_t num_records, FILE * out_file) = 0;

    uint32_t    _num_threads;
    uint64_t    _num_params_per_thread;
    uint64_t    _num_params_pgpt;
    char *      _base_file_name;
    FILE * *    _files;
};


#endif //DBX1000_CONFLICT_GRAPH_H
