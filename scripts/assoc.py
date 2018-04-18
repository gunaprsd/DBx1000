import re
import sys
if __name__ == "__main__":
    num_threads = 30
    rank = 1
    ranks_dict = {}
    for line in open(sys.argv[1], 'r'):
        if line.startswith("$"):
            line = line[2:len(line)]
        else:
            continue
        vals = re.split(', |\n', line)
        epoch = int(vals[0])
        tid = int(vals[1])
        time = float(vals[2])
        if epoch in ranks_dict:
            ranks_dict[epoch].append(time)
        else:
            ranks_dict[epoch] = [time]
        if rank == num_threads:
            rank = 1
        else:
            rank += 1

    for rank in range(0, num_threads):
        s = ""
        for key in ranks_dict:
            s += str(ranks_dict[key][rank]) + ", "
        print s
