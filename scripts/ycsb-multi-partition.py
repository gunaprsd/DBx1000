import os
# Single Partition
configs = []
for tag in ["high", "medium"]:
    for tag2 in ["mp-lr-lc", "mp-lr-mc", "mp-lr-hc", "mp-mr-lc", "mp-mr-mc", "mp-mr-hc", "mp-hr-lc", "mp-hr-mc", "mp-hr-hc"]:
        for cores in [2, 4, 8, 16, 32]:
            config = ""
            config += " -Pbycsb"
            config += " -Pt" + tag
            config += " -Pt2" + tag2
            config += " -t" + str(cores)
            config += " -s" + str(cores * 256)
            configs.append(config)


#for config in configs:
#    command = "./rundb" + config + " -Pg"
#    print(command)
#    os.system(command)

#for config in configs:
#    command = "./rundb" + config + " -Ppa -Pu5"
#    print(command)
#    os.system(command)

#for config in configs:
#    command = "./rundb" + config + " -Ppa -Pu300"
#    print(command)
#    os.system(command)


os.system("touch results/mp-no-wait-raw.txt")
os.system("touch results/mp-no-wait-u5.txt")
for config in configs:
    command = "./rundb" + config + " -s5 -Per >> results/mp-no-wait-raw.txt"
    print(command)
    os.system(command)
    command = "./rundb" + config + " -s5 -Pep -Pu5 >> results/mp-no-wait-u5.txt"
    print(command)
    os.system(command)
