import os
# Single Partition
configs = []
for tag in ["high"]:
    for cores in [30]:
        for parts in [60]:
            if parts <= 2 * cores:
                config = ""
                config += " -Pbycsb"
                config += " -Pt" + tag
                config += " -Pt2mp-custom-hc-" + str(parts)
                config += " -t" + str(cores)
                config += " -s" + str(cores * 256)
                configs.append(config)


log_file = "ycsb_stats.txt"

#for config in configs:
#    command = "./rundb" + config + " -Pg >> " + log_file
#    print(command)
#    os.system("echo " + command + " >> " + log_file)
#    os.system(command)

#for config in configs:
#    command = "./rundb" + config + " -Ppa -Pu5 >> " + log_file
#    print(command)
#    os.system("echo " + command + " >> " + log_file)
#    os.system(command)

for config in configs:
    for i in xrange(0, 5):
        command = "./rundb" + config + " -Per >> " + log_file
        print(command)
        os.system("echo " + command + " >> " + log_file)
        os.system(command)

for config in configs:
    for i in xrange(0, 5):
        command = "./rundb" + config + " -Pep -Pu5 >> " + log_file
        print(command)
        os.system("echo " + command + " >> " + log_file)
        os.system(command)
