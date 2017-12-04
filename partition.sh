base="data"
benchmark="ycsb"
for tag in "low" "medium"
do
    for core in 2 4 8 16 32
    do
	for ufactor in 30 500
	do
	    echo "Partitioning $benchmark $tag cores=$core ufactor=$ufactor"
	    folder="$base/$benchmark/$tag/c$core/partitioned/u$ufactor";
	    mkdir -p $folder;
	    ./rundb -Pp -Pb$benchmark -Pt$tag -t$core -Pu$ufactor > "$folder/info.txt"
	done
    done
done
#benchmark="tpcc"
#for tag in "wh4" "wh64"
#do
#    for core in 2 4 8 16 32
#    do
#	mkdir -p "$base/$benchmark/$tag/c$core/raw";
#	./rundb -t$core -Pb$benchmark -Pt$tag
#	for ufactor in 30 500 1000 2000
#	do
#	    mkdir -p "$base/$benchmark/$tag/c$core/partitioned/u$ufactor";
#	done
#    done
#done

	
