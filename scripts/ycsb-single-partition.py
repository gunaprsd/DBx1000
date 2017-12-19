import os
# Single Partition
configs = []
for tag in ["low", "medium", "high"]:
    for tag2 in ["sp-plc", "sp-pec", "sp-pgc"]:
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

file = "results/sp-no-wait-raw.txt"
os.system("touch " + file)
for config in configs:
    command = "./rundb" + config + " -Per -r5 >> " + file
    print(command)
    os.system(command)

file = "results/sp-no-wait-u5.txt"
os.system("touch " + file)
for config in configs:
    command = "./rundb" + config + " -Pep -Pu5 -r5 >> " + file
    print(command)
    os.system(command)

file = "results/sp-no-wait-u300.txt"
os.system("touch " + file)
for config in configs:
    command = "./rundb" + config + " -Pep -Pu300 -r5 >> " + file
    print(command)
    os.system(command)
