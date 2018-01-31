#!/bin/bash
./build/rundb -benchmark=tpcc -tpcc_num_wh=15 -threads=15 -tag=tpcc_wh15_c15 -output_folder=data/tpcc_wh15_c15_raw -task=generate >> tpcc_generate.txt
./build/rundb -benchmark=tpcc -tpcc_num_wh=15 -threads=15 -tag=tpcc_wh15_c15 -input_folder=data/tpcc_wh15_c15_raw -output_folder=data/tpcc_wh15_c15_partitioned -task=partition >> tpcc_partition.txt
