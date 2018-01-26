import os
import sys
# Single Partition
executable = "./build/rundb"
data_folder = "data"
start_num = 4
num_runs = 5
configs = {}
for cores in [15, 30]:
    for wh in [4, 8, 15, 30, 60]:
        if wh <= 2 * cores:
            config = ' -benchmark="tpcc"'
            config += ' -tpcc_num_wh=' + str(wh)
            config += ' -tpcc_wh_update'
            config += ' -threads=' + str(cores)
            tag = 'tpcc'
            tag += '_wh' + str(wh)
            tag += '_c' + str(cores)
            configs[tag] = config


def generate(start, end):
    log_file = "tpcc_generation.txt"
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
    log_file = "tpcc_partition_" + str(start) + "_" + str(end) + ".txt"
    for tag in configs.keys():
        for num in xrange(start, end):
            command = executable
            command += configs[tag]
            seed_tag = tag + '_s' + str(num)
            command += ' -tag="' + seed_tag + '"'
            command += ' -input_folder="' + data_folder + "/" + seed_tag + '_raw"'
            command += ' -output_folder="' + data_folder + "/" + seed_tag + '_partitioned"'
            command += ' -task="partition"'
            command += ' -ufactor=5'
            command += ' >> ' + log_file
            print(command)
            os.system("echo " + command + " >> " + log_file)
            os.system(command)

#for config in configs:
#    command = "./rundb" + config + " -Ppa -Pu5 >> " + log_file
#    print(command)
#    os.system("echo " + command + " >> " + log_file)
#    os.system(command)

#for config in configs:
#    for i in xrange(0, 5):
#        command = "./rundb" + config + " -Per >> " + log_file
#        print(command)
#        os.system("echo " + command + " >> " + log_file)
#        os.system(command)

#for config in configs:
#    for i in xrange(0, 5):
#        command = "./rundb" + config + " -Pep -Pu5 >> " + log_file
#        print(command)
#        os.system("echo " + command + " >> " + log_file)
#        os.system(command)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Error!")
    elif sys.argv[1] == "generate":
        generate(int(sys.argv[2]), int(sys.argv[3]))
    elif sys.argv[1] == "partition":
        partition(int(sys.argv[2]), int(sys.argv[3]))
    else:
        assert(False)
