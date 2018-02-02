import os
import sys
# Single Partition
executable = "./build/rundb"
data_folder = "data"
start_num = 4
num_runs = 5
configs = {}
for cores in [15]:
    for parts in [30, 15, 8, 4]:
        if parts <= 2 * cores:
            config = ' -benchmark="ycsb"'
            config += ' -ycsb_zipf_theta=0.9'
            config += ' -ycsb_read_percent=0.5'
            config += ' -ycsb_multipart_txns=0'
            config += ' -ycsb_num_partitions=' + str(parts)
            config += ' -threads=' + str(cores)
            tag = 'ycsb_single_high'
            tag += '_p' + str(parts)
            tag += '_c' + str(cores)
            configs[tag] = config


def generate(start, end):
    log_file = "ycsb_single_generation.txt"
    for tag in configs.keys():
        for num in xrange(start, end):
            command = executable
            command += configs[tag]
            seed_tag = tag + '_s' + str(num)
            command += ' -tag="' + seed_tag + '"'
            command += ' -output_folder="' + data_folder + "/" + seed_tag + '_raw"'
            command += ' -task="generate"'
            command += ' -seed=' + str(num)
            command += ' >> ' + log_file
            print(command)
            os.system("echo " + command + " >> " + log_file)
            os.system(command)

def partition(start, end):
    log_file = "ycsb_single_partition_" + str(start) + "_" + str(end) + ".txt"
    for tag in configs.keys():
        for num in xrange(start, end):
            command = executable
            command += configs[tag]
            seed_tag = tag + '_s' + str(num)
            command += ' -tag="' + seed_tag + '"'
            command += ' -input_folder="' + data_folder + "/" + seed_tag + '_raw"'
            command += ' -output_folder="' + data_folder + "/" + seed_tag + '_partitioned"'
            command += ' -task="partition"'
            command += ' -objtype=edge_cut'
            command += ' -ufactor=10'
            command += ' >> ' + log_file
            print(command)
            os.system("echo " + command + " >> " + log_file)
            os.system(command)

def execute(start, end, type_tag):
    log_file = "ycsb_single_execution_" + str(start) + "_" + str(end) + ".txt"
    for tag in configs.keys():
        for num in xrange(start, end):
            base_command = executable
            base_command += configs[tag]
            seed_tag = tag + '_s' + str(num)
            base_command += ' -tag="' + seed_tag + '"'
            base_command += ' -task="execute"'
            # raw command
            command = base_command
            command += ' -input_folder="' + data_folder + "/" + seed_tag + '_' + type_tag + '"'
            command += ' >> ' + log_file
            for i in xrange(0, 5):
                print(command)
                os.system("echo " + command + " >> " + log_file)
                os.system(command)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Error!")
    elif sys.argv[1] == "generate":
        generate(int(sys.argv[2]), int(sys.argv[3]))
    elif sys.argv[1] == "partition":
        partition(int(sys.argv[2]), int(sys.argv[3]))
    elif sys.argv[1] == "execute":
        assert(len(sys.argv) == 5)
        execute(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4])
    else:
        assert(False)
