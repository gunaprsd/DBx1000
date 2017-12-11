base="data"
benchmark="ycsb"
for tag in "low" "medium" "high"
do
    for core in 2 4 8 16 32
    do
	echo "Generating $benchmark $tag $core"
	mkdir -p "$base/$benchmark/$tag/c$core/raw";
	./rundb -t$core -Pb$benchmark -Pt$tag
    done
done
benchmark="tpcc"
for tag in "wh4" "wh64"
do
    for core in 2 4 8 16 32
    do
	echo "Generating $benchmark $tag $core"
#	mkdir -p "$base/$benchmark/$tag/c$core/raw";
#	./rundb -t$core -Pb$benchmark -Pt$tag
    done
done

	
