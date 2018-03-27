import os
import sys
# Single Partition
executable = "./build/rundb"
data_folder = "data"
start_num = 4
num_runs = 5
configs = []
sizes =  [256000]
for cores in [30]:
    for parts in [30]:
        for size_per_thread in sizes:
            if parts != cores:
                continue
            config = ' -benchmark=ycsb \\\n'
            config += ' -ycsb_zipf_theta=0.9 \\\n'
            config += ' -ycsb_read_percent=0.5 \\\n'
            config += ' -ycsb_multipart_txns=0 \\\n'
            config += ' -ycsb_num_partitions=' + str(parts) + ' \\\n'
            config += ' -threads=' + str(cores) + ' \\\n'
            config += ' -size_per_thread=' + str(size_per_thread) + ' \\\n'
            tag = 'ycsb'
            tag += '_p' + str(parts)
            tag += '_c' + str(cores)
            tag += '_sz' + str(size_per_thread)
            configs.append({'tag':tag, 'config':config})

def generate(start, end):
    log_file = "ycsb_generation.txt"
    for pr in configs:
        tag = pr['tag']
        config = pr['config']
        for num in xrange(start, end):
            command = executable +  ' \\\n'
            command += config
            seed_tag = tag + '_s' + str(num)
            command += ' -tag=' + seed_tag + ' \\\n'
            command += ' -output_folder=' + data_folder + '/' + seed_tag + '_raw \\\n'
            command += ' -task=generate \\\n'
            command += ' -seed=' + str(num) + ' \\\n'
            command += ' >> ' + log_file
            print(command)
            os.system("echo " + command + " >> " + log_file)
            os.system(command)

def partition(start, end, type_tag):
    log_file = "ycsb_partition_" + type_tag + "_" + str(start) + "_" + str(end) + ".txt"
    for pr in configs:
        tag = pr['tag']
        config = pr['config']
        for num in xrange(start, end):
            command = executable + ' \\\n'
            command += config
            seed_tag = tag + '_s' + str(num)
            command += ' -tag=' + seed_tag + ' \\\n'
            command += ' -input_folder=' + data_folder + "/" + seed_tag + '_raw \\\n'
            command += ' -output_folder=' + data_folder + "/" + seed_tag + '_' + type_tag + ' \\\n'
            command += ' -task=partition \\\n'
            command += ' -parttype=' + type_tag + '\\\n'
            command += ' -ufactor=100 \\\n'
            command += ' -iterations=30 \\\n'
            command += ' >> ' + log_file
            print(command)
            os.system("echo " + command + " >> " + log_file)
            os.system(command)

def execute(start, end, type_tag, scheduler_tag):
    log_file = "ycsb_execution_occ_" + type_tag + "_" + scheduler_tag + "_" + str(start) + "_" + str(end) + ".txt"
    for pr in configs:
        tag = pr['tag']
        config = pr['config']
        for num in xrange(start, end):
            base_command = executable + ' \\\n'
            base_command += config
            seed_tag = tag + '_s' + str(num)
            base_command += ' -tag=' + seed_tag + ' \\\n'
            base_command += ' -task=execute \\\n'
            # raw command
            command = base_command
            command += ' -input_folder=' + data_folder + "/" + seed_tag + '_' + type_tag + ' \\\n'
            command += ' -scheduler_type=' + scheduler_tag + ' \\\n'
            command += ' >> ' + log_file
            for i in xrange(0, 10):
                print(command)
                os.system("echo " + command + " >> " + log_file)
                os.system(command)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Error!")
    elif sys.argv[1] == "generate":
        generate(int(sys.argv[2]), int(sys.argv[3]))
    elif sys.argv[1] == "partition":
        assert(len(sys.argv) == 5)
        partition(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4])
    elif sys.argv[1] == "execute":
        assert(len(sys.argv) == 6)
        execute(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], sys.argv[5])
    else:
        assert(False)
