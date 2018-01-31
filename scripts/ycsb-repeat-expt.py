import os
executable = "./build/rundb"
data_folder = "data"
parts = 30
cores = 30
seed = 0
log_file="repeat_debug.txt"
for i in xrange(1,25):
    config = ' -benchmark="ycsb"'
    config += ' -ycsb_zipf_theta=0.9'
    config += ' -ycsb_read_percent=0.5'
    config += ' -ycsb_multipart_txns=0'
    config += ' -ycsb_num_partitions=' + str(parts)
    config += ' -threads=' + str(cores)
    tag = 'ycsb_single_high'
    tag += '_p' + str(parts)
    tag += '_c' + str(cores)

    command = executable
    command += config
    command += ' -tag="' + tag + '"'
    command += ' -input_folder="' + data_folder + "/" + tag + '_raw"'
    command += ' -output_folder="' + data_folder + "/" + tag + '_partitioned_' + str(i) + '"'
    command += ' -task="partition"'
    command += ' -ufactor=5'
    command += ' >> ' + log_file
    print(command)
    os.system("echo " + command + " >> " + log_file)
    os.system(command)
