```

	change pc name on this file every swap in:
 	vim	/proj/ucare/git/hadoop-ucare/psbin/mini-slaves

 	cd /proj/ucare/riza/start_script/
	./setup-HDFS-395-DAN.sh
	cd /mnt/extra/hadoop/
	./runbench.sh

	git pull ucare-github-dan master
	vim runbench.sh
```


note:

- DFSConfigKeys.java for configuration
- hadoop conf at " echo $HADOOP_CONF_DIR "


main()
	runBenchmark(new HdfsConfiguration(), new ArrayList<String>(Arrays.asList(args)))
		NNThroughputBenchmark bench = null;
			opStat = bench.new CreateFileStats(args);
			opStat = bench.new OpenFileStats(args);
			opStat = bench.new DeleteFileStats(args);
			opStat = bench.new FileStatusStats(args);
			opStat = bench.new RenameFileStats(args);
			opStat = bench.new BlockReportStats(args);
			opStat = bench.new ReplicationStats(args);
			opStat = bench.new CleanAllStats(args);
			ops.add(opStat);
			// run each benchmark
		      for(OperationStatsBase op : ops) {
		        LOG.info("Starting benchmark: " + op.getOpName());
		        op.benchmark();
		        op.cleanUp();
		      }
		bench.close();

