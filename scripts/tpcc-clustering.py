import os
import sys
# Single Partition
executable = "./build/rundb"
data_folder = "data"
start_num = 4
num_runs = 5
size_per_thread=1000
configs = []
name = {0 : 'pymnt', 0.5 : 'mixed', 1: 'order'}
for cores in [30]:
    for perc_payment in [0.5]:
        for wh in [60, 30, 15, 8, 4]:
            if wh <= 2 * cores:
                config = ' -benchmark=tpcc'
                config += ' -tpcc_num_wh=' + str(wh)
                config += ' -tpcc_wh_update'
                config += ' -tpcc_perc_payment=' + str(perc_payment)
                config += ' -threads=' + str(cores)
                config += ' -size_per_thread=' + str(size_per_thread)
                tag = 'tpcc'
                tag += '_wh' + str(wh)
                tag += '_c' + str(cores)
                tag += '_' + name[perc_payment]
                configs.append({'tag':tag, 'config':config})

def generate(start, end):
    log_file = "tpcc_generation.txt"
    for pr in configs:
        tag = pr['tag']
        config = pr['config']
        for num in xrange(start, end):
            command = executable
            command += config
            seed_tag = tag + '_s' + str(num)
            command += ' -tag=' + seed_tag
            command += ' -output_folder=' + data_folder + '/' + seed_tag + '_raw'
            command += ' -task=generate'
            command += ' -seed=' + str(num)
            command += ' >> ' + log_file
            print(command)
            os.system("echo " + command + " >> " + log_file)
            os.system(command)

def partition(start, end):
    log_file = "tpcc_ec_conflictwgts_u10" + str(start) + "_" + str(end) + ".txt"
    for pr in configs:
        tag = pr['tag']
        config = pr['config']
        for num in xrange(start, end):
            command = executable
            command += config
            seed_tag = tag + '_s' + str(num)
            command += ' -tag=' + seed_tag
            command += ' -input_folder=' + data_folder + '/' + seed_tag + '_raw'
            command += ' -output_folder=' + data_folder + '/' + seed_tag + '_partitioned'
            command += ' -task=partition'
            command += ' -ufactor=10'
            command += ' -objtype=edge_cut'
            command += ' -nounit_weights'
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
