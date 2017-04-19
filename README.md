```
	connect ssh with node-0
	change pc name of node-0 on this file every swap in:
 	vim	/proj/ucare/git/hadoop-ucare/psbin/mini-slaves

script:

 	cd /proj/ucare/riza/start_script/
	./setup-HDFS-395-DAN.sh
	cd /mnt/extra/hadoop/
	./runbench.sh

	git checkout runbench.sh
	git pull ucare-github-dan master
	vim runbench.sh

	The config is in echo $HADOOP_CONF_DIR directory
	cd /proj/ucare/git/hadoop-ucare/psbin/ucare_se_conf/hadoop-etc/hadoop-2.7.1

	fork dulu dari main reponya 

	cloning repo:
	git clone -b ucare-HDFS-395 --single-branch https://github.com/daniarherikurniawan/hadoop.git

	git checkout -b ucare-HDFS-395-DAN
	git push --set-upstream origin ucare-HDFS-395-DAN

	find . -name "*.DS_Store" -type f -delete
```


note:

- DFSConfigKeys.java for configuration
- hadoop conf at " echo $HADOOP_CONF_DIR "


main()
	runBenchmark(new HdfsConfiguration(), new ArrayList<String>(Arrays.asList(args)))
		NNThroughputBenchmark bench = null;
			opStat = bench.new CreateFileStats(args); // not used
			opStat = bench.new OpenFileStats(args); // not used
			opStat = bench.new DeleteFileStats(args); // not used
			opStat = bench.new FileStatusStats(args); // not used
			opStat = bench.new RenameFileStats(args); // not used
			opStat = bench.new BlockReportStats(args);
			opStat = bench.new ReplicationStats(args); // not used
			opStat = bench.new CleanAllStats(args); // not used
			ops.add(opStat);

			// run each benchmark
		      for(OperationStatsBase op : ops) {
		        LOG.info("Starting benchmark: " + op.getOpName());
		        op.benchmark();
		        	bench.new BlockReportStats(args);
		        		benchmark()

		        			Datanode is registered at DatanodeRegistration
		        				create datanodes threads as many as the number of -datanodes 
		        				generateInputs(opsPerThread); on BlockReportStats
		        				datanodes = new TinyDatanode[nrDatanodes];

		        				for(int idx=0; idx < nrDatanodes; idx++) {
							        datanodes[idx] = new TinyDatanode(idx, blocksPerReport);

							        	TinyDatanode(int dnIdx, int blockCapacity) throws IOException {
									      dnRegistration = new DatanodeRegistration(getNodeName(dnIdx));
									      this.blocks = new ArrayList<Block>(blockCapacity);
									      this.capacity = blockCapacity;
									      this.nrBlocks = 0;
									    }

							        datanodes[idx].register();
							        assert datanodes[idx].getName().compareTo(prevDNName) > 0
							          : "Data-nodes must be sorted lexicographically.";
							        datanodes[idx].sendHeartbeat();
							        prevDNName = datanodes[idx].getName();

							        ExtendedBlock lastBlock = addBlocks(fileName, clientName);

							        for(int jdx = 0; jdx < blocksPerFile; jdx++) {
								        LocatedBlock loc = nameNode.addBlock(fileName, clientName, prevBlock, null);
								        prevBlock = loc.getBlock();
								        for(DatanodeInfo dnInfo : loc.getLocations()) {
								          int dnIdx = Arrays.binarySearch(datanodes, dnInfo.getName());
								          //LOG.info("Placing block "+dnInfo+" to datanode "+dnIdx);
								          datanodes[dnIdx].addBlock(loc.getBlock().getLocalBlock());
								          nameNode.blockReceived(
								              datanodes[dnIdx].dnRegistration, 
								              loc.getBlock().getBlockPoolId(),
								              new Block[] {loc.getBlock().getLocalBlock()},
								              new String[] {""});
								        }
							    }

							    namenode.NNThroughputBenchmark: Creating 1067(could be more) with 1 blocks each. Actually: numOfBlock * numOfDataNode
							    
							    finished:  
									namenode.NNThroughputBenchmark: nrBlocks = 43
									namenode.NNThroughputBenchmark: nrBlocks = 47
									namenode.NNThroughputBenchmark: nrBlocks = 46
									namenode.NNThroughputBenchmark: nrBlocks = 47
									namenode.NNThroughputBenchmark: nrBlocks = 46
									namenode.NNThroughputBenchmark: nrBlocks = 49

						    	create daemons to simulate each datanodes cluster
						    		daemons.add(new StatsDaemon(tIdx, opsPerThread[tIdx], this));

						    		void benchmarkOne() throws IOException {
								      for(int idx = 0; idx < opsPerThread; idx++) {
								        if((localNumOpsExecuted+1) % statsOp.ugcRefreshCount == 0)
								          nameNode.refreshUserToGroupsMappings();
								        long stat = statsOp.executeOp(daemonId, idx, arg1);
								        localNumOpsExecuted++;
								        localCumulativeTime += stat;
								      }
								    }		

		        op.cleanUp();
		      }
		bench.close();

[2/26/17, 10:35:00 PM] Riza Suminto: https://github.com/apache/hadoop/blob/73451ed2d9fb5eb228d80ad5f3be302a60496527/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java
[2/26/17, 10:35:36 PM] Riza Suminto: In processReport
[2/26/17, 10:36:26 PM] Riza Suminto: Can you add log to print size of each list resulted from reportDiff?


edited:
- NetworkTopology (contain)
- 

17000
19000

1574027

6382
7273
8335
9572
12237

27500
27000

56000
55000

date
93.188.160.233


sci
89.38.147.133


generatesInput -> in the last it sent prepare block reports (formBlockReport)





if port 9000 got a problem :
- netstat -tulpn
- kill -9 39871

check :
- BlockListAsLongs
