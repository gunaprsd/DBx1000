base="data"
benchmark="ycsb"
tag="low"
for i in 1 2 3 4 5
do
    for alg in NO_WAIT
    do
	touch "results_no_wait/$i-$tag-$alg-results.txt"
	for core in 2 4 8
	do
	    echo "Running $benchmark $tag raw on $core cores with $alg" >> "results_no_wait/$i-$tag-$alg-results.txt"
	    ./rundb_$alg -t$core -Per -Pb$benchmark -Pt$tag >> "results_no_wait/$i-$tag-$alg-results.txt"
	done

	ufactor=30;
	for core in 2 4 8
	do
	    echo "Running $benchmark $tag partitioned $ufactor on $core cores with $alg" >> "results_no_wait/$i-$tag-$alg-results.txt"
	    ./rundb_$alg -t$core -Pep -Pb$benchmark -Pt$tag -Pu$ufactor >> "results_no_wait/$i-$tag-$alg-results.txt"
	done
	ufactor=500;
	for core in 2 4
	do
	    echo "Running $benchmark $tag partitioned $ufactor on $core cores with $alg" >> "results_no_wait/$i-$tag-$alg-results.txt"
	    ./rundb_$alg -t$core -Pep -Pb$benchmark -Pt$tag -Pu$ufactor >> "results_no_wait/$i-$tag-$alg-results.txt"
	done
    done
done
