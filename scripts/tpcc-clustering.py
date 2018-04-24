import os
import sys
# Single Partition
executable = "./build/rundb"
data_folder = "data"
start_num = 4
num_runs = 5
size_per_thread = 3000000
configs = []
name = {0: 'order', 0.5: 'mixed', 1: 'pymnt'}
for cores in [15]:
    for perc_payment in [0, 0.5, 1]:
        for wh in [15]:
            config = ' -benchmark=tpcc \\\n'
            config += ' -tpcc_num_wh=' + str(wh) + ' \\\n'
            config += ' -tpcc_wh_update \\\n'
            config += ' -tpcc_perc_payment=' + str(perc_payment) + ' \\\n'
            config += ' -threads=' + str(cores) + ' \\\n'
            config += ' -size_per_thread=' + str(size_per_thread) + ' \\\n'
            tag = 'tpcc'
            tag += '_wh' + str(wh)
            tag += '_c' + str(cores)
            tag += '_' + name[perc_payment]
            tag += '_sz' + str(size_per_thread)
            configs.append({'tag': tag, 'config': config})


def generate(start, end):
    log_file = "tpcc_generation.txt"
    for pr in configs:
        tag = pr['tag']
        config = pr['config']
        for num in xrange(start, end):
            command = executable + ' \\\n'
            command += config
            seed_tag = tag + '_s' + str(num)
            command += ' -tag=' + seed_tag + ' \\\n'
            command += ' -output_file=' + data_folder + '/' + seed_tag + '_raw.dat \\\n'
            command += ' -task=generate \\\n'
            command += ' -seed=' + str(num) + ' \\\n'
            command += ' >> ' + log_file
            print(command)
            os.system("echo " + command + " >> " + log_file)
            os.system(command)


def partition(start, end):
    log_file = "tpcc_approx_u10" + str(start) + "_" + str(end) + ".txt"
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
            command += ' -parttype=approx'
            command += ' -nounit_weights'
            command += ' >> ' + log_file
            print(command)
            os.system("echo " + command + " >> " + log_file)
            os.system(command)


def execute(start, end, type_tag, scheduler_tag):
    log_file = "tpcc_execution.txt"
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
            command += ' -input_file=' + data_folder + \
                "/" + seed_tag + '_' + type_tag + '.dat \\\n'
            command += ' -scheduler_type=' + scheduler_tag + ' \\\n'
            command += ' -abort_buffer \\\n'
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
        partition(int(sys.argv[2]), int(sys.argv[3]))
    elif sys.argv[1] == "execute":
        assert(len(sys.argv) == 6)
        execute(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], sys.argv[5])
    else:
        assert(False)
