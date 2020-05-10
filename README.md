# PS_MWC

1. build the jar 

    run the command below under directory /PS_MWC:

   ` ./gradlew build`

2. start ZooKeeper server 
(modify `conf/zoo.cfg` set `maxClientCnxns=90`)

   `cd $ZooKeeperHome/bin`
    
   ` sudo zkServer.sh start`
    
3. run on local machine

    `spark-submit --class mwcp.spark.MWCP --master local --deploy-mode client --num-executors 6 --executor-cores 2 --executor-memory 1g build/libs/PS_MWC-1.0-SNAPSHOT.jar data/bio-dmela.mtx 0 localhost false 2 2`

```
    args[0]: graph data file name;
    args[1]: experimentID
        //0: partition based on degree and then vertex id; using Zookeeper;
        //1: withOutPartition
        //2: partition based on vertex id
        //3: not using Zookeeper
    args[2]: zooKeeper server IP
    args[3]: reduce the graph after getQuickMWCL or not
    args[4]: the number of partitions of edgesRDD
    args[5]: mapPartition experimentID
        //0: get results using map insteadOf mapPartition; do not repartitionAndSortWithinPartitions
        //1: get results using mapPartition; do not repartitionAndSortWithinPartitions
        //2: get results using mapPartition; use repartitionAndSortWithinPartitions and sort by upperBound
        //3: get results using mapPartition; use repartitionAndSortWithinPartitions and sort by size of HNbSet
```
        
4. run on aws (upload .jar to a s3 bucket)
```
    spark-submit --deploy-mode cluster --class mwcp.spark.MWCP --executor-memory 8G --driver-memory 5G --executor-cores 8 --num-executors 8 s3://uhdaisqian/app/PS_MWC-1.0-SNAPSHOT.jar s3://uhdais/output/soc-orkut-dir-4 0 34.209.44.36 false 128 0
    
    spark-submit --deploy-mode client --class util.GenerateGraphForExperiment --executor-memory 5G --driver-memory 8G --executor-cores 8 --num-executors 8 s3://uhdaisqian/app/PS_MWC-1.0-SNAPSHOT.jar s3://uhdais/data/soc-orkut-dir.edges s3://uhdais/output/soc-orkut-dir-
    
    aws emr add-steps --cluster-id j-ANLBK3N1D0IF --steps Type=Spark,Name="MWCP",ActionOnFailure=CONTINUE,Args=[--class,mwcp.spark.MWCP,--driver-memory,5G,--executor-memory,8G,--executor-cores,8,--num-executors,8,s3://uhdais/applications/PS_MWC-1.0-SNAPSHOT.jar,s3://uhdais/data/ca-hollywood-2009.mtx,0,18.236.223.233,false,64,0]
 ```
 
 5. an example:
 
 ```
 MacBook-Pro-2:PS_MWC qiuqian$ spark-submit --class mwcp.spark.MWCP --master local --deploy-mode client --num-executors 6 --executor-cores 2 --executor-memory 1g build/libs/PS_MWC-1.0-SNAPSHOT.jar data/bio-dmela.mtx 0 localhost false 2 2
 Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
 20/05/10 17:11:17 INFO MWCP: Default Parallelism = 1
 20/05/10 17:11:21 INFO GetQuickMWCL: Quickly get a mwcl: List(399, 154) 355.0 in 723 ms
 Starting ZK:
 Creating znode_weight: /MWCL
 Starting ZK:
 20/05/10 17:11:22 INFO Partition2: ubDegreeGraph generated
 20/05/10 17:11:22 INFO Partition2: ubDegreeGraph generated
 20/05/10 17:11:22 INFO Partition2: ubDegreeGraph generated
 Starting ZK:
 20/05/10 17:11:24 INFO FastWclqZK: Maximum weight in zk: 355.0
 20/05/10 17:11:24 INFO FastWclqZK: ------------379.0 List(1080, 150, 346)
 Before Update: 355.0
 NewValue: 379.0
 20/05/10 17:11:24 INFO FastWclqZK: Update zk_mwcl on watcher, before: 355.0, after: 379.0
 20/05/10 17:11:24 INFO FastWclqZK: ------------398.0 List(1099, 150, 346)
 Before Update: 379.0
 NewValue: 398.0
 20/05/10 17:11:24 INFO FastWclqZK: Update zk_mwcl on watcher, before: 379.0, after: 398.0
 20/05/10 17:11:24 INFO FastWclqZK: ------------451.0 List(552, 150, 346)
 Before Update: 398.0
 NewValue: 451.0
 20/05/10 17:11:24 INFO FastWclqZK: Update zk_mwcl on watcher, before: 398.0, after: 451.0
 20/05/10 17:11:24 INFO FastWclqZK: ------------632.0 List(2153, 1579, 150, 346)
 Before Update: 451.0
 NewValue: 632.0
 20/05/10 17:11:24 INFO FastWclqZK: Update zk_mwcl on watcher, before: 451.0, after: 632.0
 20/05/10 17:11:24 INFO FastWclqZK: ------------724.0 List(167, 1378, 785, 190)
 Before Update: 632.0
 NewValue: 724.0
 20/05/10 17:11:24 INFO FastWclqZK: Update zk_mwcl on watcher, before: 632.0, after: 724.0
 20/05/10 17:11:24 INFO FastWclqZK: ------------728.0 List(785, 1378, 2771, 190)
 Before Update: 724.0
 NewValue: 728.0
 20/05/10 17:11:24 INFO FastWclqZK: Update zk_mwcl on watcher, before: 724.0, after: 728.0
 20/05/10 17:11:24 INFO MWCP: mwcl1:632.0
 20/05/10 17:11:24 INFO MWCP: mwcl2:728.0
 20/05/10 17:11:24 INFO FastWclqZK: ------------769.0 List(785, 1378, 2153, 167, 81)
 Before Update: 728.0
 NewValue: 769.0
 20/05/10 17:11:24 INFO FastWclqZK: Update zk_mwcl on watcher, before: 728.0, after: 769.0
 20/05/10 17:11:24 INFO FastWclqZK: ------------778.0 List(150, 1579, 2153, 320, 1171)
 Before Update: 769.0
 NewValue: 778.0
 20/05/10 17:11:24 INFO FastWclqZK: Update zk_mwcl on watcher, before: 769.0, after: 778.0
 20/05/10 17:11:24 INFO MWCP: mwcl1:769.0
 20/05/10 17:11:24 INFO MWCP: mwcl2:778.0
 20/05/10 17:11:24 INFO FastWclqZK: ------------805.0 List(785, 1378, 1364, 320, 2153)
 Before Update: 778.0
 NewValue: 805.0
 20/05/10 17:11:24 INFO FastWclqZK: Update zk_mwcl on watcher, before: 778.0, after: 805.0
 20/05/10 17:11:24 INFO MWCP: mwcl1:778.0
 20/05/10 17:11:24 INFO MWCP: mwcl2:805.0
 [Stage 39:=============================>                            (1 + 1) / 2]20/05/10 17:11:24 INFO MWCP: mwcl1:728.0
 20/05/10 17:11:24 INFO MWCP: mwcl2:805.0
 20/05/10 17:11:24 INFO MWCP: Reduced 3854 subProblems while partitioning
 20/05/10 17:11:24 INFO MWCP: Maximum Weight Clique: List(785, 1378, 1364, 320, 2153), Weight: 805.0
 20/05/10 17:11:24 INFO MWCP: Time Cost: 3860 ms
 20/05/10 17:11:24 INFO MWCP: original vertices num: 7393
 20/05/10 17:11:24 INFO MWCP: reducedSubPar: 3854
 20/05/10 17:11:24 INFO MWCP: reducedSubSolveStep: 3479
 20/05/10 17:11:24 INFO MWCP: reducedSubTotal: 7333
 20/05/10 17:11:24 INFO MWCP: solvedSubProbemNumber: 60
 20/05/10 17:11:24 INFO MWCP: solvedSubProbemNumber_halfPruned: 60
 20/05/10 17:11:24 INFO MWCP: bio-dmela.mtx_0
```
