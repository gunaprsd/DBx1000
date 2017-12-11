base="data"
benchmark="ycsb"
tag="medium"
for core in 2 4 8 16 32
do
    for ufactor in 30 500 1000 2000
    do
	echo "Partitioning $benchmark $tag cores=$core ufactor=$ufactor"
	folder="$base/$benchmark/$tag/c$core/partitioned/u$ufactor";
	mkdir -p $folder;
	./rundb -Pp -Pb$benchmark -Pt$tag -t$core -Pu$ufactor > "$folder/info.txt"
    done
done
