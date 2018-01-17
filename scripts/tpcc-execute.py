import os
# TPCC
configs = []
for tag in ["wh4", "wh8", "wh15"]:
    for cores in [15]:
        config = ""
        config += " -Pbtpcc"
        config += " -Pt" + tag
        config += " -t" + str(cores)
        config += " -s" + str(cores * 256)
        configs.append(config)

#for config in configs:
#    command = "./rundb" + config + " -Pg"
#    print(command)
#    os.system(command)

#os.system("touch tpcc_partition_stats.txt")
#for config in configs:
#    command = "./rundb" + config + " -Ppa -Pu5 >> tpcc_partition_stats.txt"
#    print(command)
#    os.system(command)

cc= "mvcc"
cores = 15
file_name = "tpcc_" + cc + "_" + str(cores) + ".txt"
os.system("touch " + file_name)
for config in configs:
    for i in xrange(0,5):
        command = "./rundb" + config + " -Per >> " + file_name
        print(command)
        os.system(command)

for config in configs:
    for i in xrange(0,5):
        command = "./rundb" + config + " -Pep -Pu5 >> " + file_name
        print(command)
        os.system(command)
