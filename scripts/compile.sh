for alg in NO_WAIT DL_DETECT OCC MVCC SILO
do
    make ALG=$alg
    mv rundb rundb_$alg
done	
