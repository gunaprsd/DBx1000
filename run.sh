base="data"
benchmark="ycsb"
tag="low"
for alg in NO_WAIT DL_DETECT MVCC OCC SILO
do
    touch "results/$tag-$alg-results.txt"
    for core in 2 4 8
    do
	echo "Running $benchmark $tag raw on $core cores with $alg" >> "results/$tag-$alg-results.txt"
	./rundb_$alg -t$core -Per -Pb$benchmark -Pt$tag >> "results/$tag-$alg-results.txt"
    done

    ufactor=30;
    for core in 2 4 8
    do
	echo "Running $benchmark $tag partitioned $ufactor on $core cores with $alg" >> "results/$tag-$alg-results.txt"
	./rundb_$alg -t$core -Pep -Pb$benchmark -Pt$tag -Pu$ufactor >> "results/$tag-$alg-results.txt"
    done
    ufactor=500;
    for core in 2 4
    do
	echo "Running $benchmark $tag partitioned $ufactor on $core cores with $alg" >> "results/$tag-$alg-results.txt"
	./rundb_$alg -t$core -Pep -Pb$benchmark -Pt$tag -Pu$ufactor >> "results/$tag-$alg-results.txt"
    done
done

tag="medium"
for alg in NO_WAIT DL_DETECT MVCC OCC SILO
do
    touch "results/$tag-$alg-results.txt"
    for core in 2 4 8 16 32
    do
	echo "Running $benchmark $tag raw on $core cores with $alg" >> "results/$tag-$alg-results.txt"
	./rundb_$alg -t$core -Per -Pb$benchmark -Pt$tag >> "results/$tag-$alg-results.txt"
    done

    for core in 2 4 8 16
    do
	for ufactor in 30 500 1000 2000
	do
	    echo "Running $benchmark $tag partitioned $ufactor on $core cores with $alg" >> "results/$tag-$alg-results.txt"
	    ./rundb_$alg -t$core -Pep -Pb$benchmark -Pt$tag -Pu$ufactor >> "results/$tag-$alg-results.txt"
	done
    done
    
    core=32;
    for ufactor in 30 1000
    do
	echo "Running $benchmark $tag partitioned $ufactor on $core cores with $alg">> "results/$tag-$alg-results.txt"
	./rundb_$alg -t$core -Pep -Pb$benchmark -Pt$tag -Pu$ufactor >> "results/$tag-$alg-results.txt"
    done
done

tag="high"
for alg in NO_WAIT DL_DETECT MVCC OCC SILO
do
    touch "results/$tag-$alg-results.txt"
    for core in 2 4
    do
	echo "Running $benchmark $tag raw on $core cores with $alg" >> "results/$tag-$alg-results.txt"
	./rundb_$alg -t$core -Per -Pb$benchmark -Pt$tag >> "results/$tag-$alg-results.txt"
    done

    for core in 2
    do
	for ufactor in 30 500 1000 2000
	do
	    echo "Running $benchmark $tag partitioned $ufactor on $core cores with $alg" >> "results/$tag-$alg-results.txt"
	    ./rundb_$alg -t$core -Pep -Pb$benchmark -Pt$tag -Pu$ufactor >> "results/$tag-$alg-results.txt"
	done
    done

    for core in 4
    do
	for ufactor in 30 500 1000
	do
	    echo "Running $benchmark $tag partitioned $ufactor on $core cores with $alg" >> "results/$tag-$alg-results.txt"
	    ./rundb_$alg -t$core -Pep -Pb$benchmark -Pt$tag -Pu$ufactor >> "results/$tag-$alg-results.txt"
	done
    done
done
