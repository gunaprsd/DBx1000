import re
import sys


def get_value(s):
    ps = s.split('=')
    return ps[1]


def parse_info(filename):
    all_info = {}
    current_run_info = {}
    run = 1
    for line in open(filename, 'r'):
        if(line.startswith("[tid=")):
            pairs = re.split(", | ", line)
            tid = pairs[0][5: len(pairs[0]) - 1]
            tu = pairs[3]
            tf = pairs[4]
            tb = pairs[5]
            te = pairs[6]
            current_run_info["union"] = float(get_value(tu))
            current_run_info["find"] = float(get_value(tf))
            current_run_info["blocked"] = float(get_value(tb))
            current_run_info["execute"] = float(get_value(te))
            if int(tid) == 29:
                all_info[run] = current_run_info
                current_run_info = {}
                run += 1
    return all_info


def find_stats(all_info):
    num_types = 6
    num_runs_per_type = 10
    for type_ in range(0, num_types):
        sums = {}
        for run in range(1, num_runs_per_type + 1):
            runid = type_ * num_runs_per_type + run
            info = all_info[runid]
            for key in info:
                if run == 1:
                    sums[key] = 0.0
                sums[key] += info[key]
        for key in sums:
            sums[key] /= num_runs_per_type
        res = str(sums["union"]) + ", " + str(sums["find"]) + \
            ", " + str(sums["blocked"]) + ", " + str(sums["execute"])
        print res


if __name__ == "__main__":
    info = parse_info(sys.argv[1])
    find_stats(info)
