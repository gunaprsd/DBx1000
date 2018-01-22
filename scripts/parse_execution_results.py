import sys

to_print_list = ['txn_cnt', 'abort_cnt', 'abort_rate', 'throughput', 'execution_time', 'time_index', 'time_man', 'time_abort', 'time_cleanup', 'run_time']

def pretty_print(perf_dict):
    line = ""
    for id in to_print_list:
	if id == 'abort_rate':
		abort_cnt= float(perf_dict['abort_cnt'])
		txn_cnt = float(perf_dict['txn_cnt'])
		line += str(abort_cnt/(abort_cnt + txn_cnt))
	else:
	        line += perf_dict[id]
	line += ', '
    print(line)

def print_avg_line(dicts):
    assert(len(dicts) == 5)
    tot_dict = {}
    for id in to_print_list:
        tot_dict[id] = 0.0

    for dict in dicts:
        for id in to_print_list:
	    if id == 'abort_rate':
		abort_cnt= float(dict['abort_cnt'])
		txn_cnt = float(dict['txn_cnt'])
                tot_dict[id] += abort_cnt/(abort_cnt + txn_cnt)
	    else:
	        tot_dict[id] += float(dict[id])
    line = ""
    for id in to_print_list:
        line += str(tot_dict[id] / 5.0);
        line += ","
    print(line)

def parse_file(filename):
    f = open(filename, 'r')
    dicts = []
    perf_dict = {}
    cnt = 0
    for line in f:
        if line.startswith('Total Runtime'):
            perf_dict.clear()
            exec_time = line.split(' ')[3]
            perf_dict['execution_time'] = exec_time
        elif line.startswith('[summary]'):
            kvpairs_list_str = line[9:]
            kvpairs = kvpairs_list_str.split(',')
            for pair in kvpairs:
	        tokens = pair.split('=')
	        perf_dict[tokens[0].strip()] = tokens[1].strip()
            #pretty_print(perf_dict)
            dicts.append(perf_dict)
            cnt += 1
            if cnt % 5 == 0:
	        print_avg_line(dicts)
                dicts = []

    f.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Please specify file name"
    else:
        parse_file(sys.argv[1])
