spark-submit --deploy-mode cluster --class mwcp.spark.MWCP --executor-memory 8G --driver-memory 5G --executor-cores 8 --num-executors 8 s3://qiansapp/MWCP_SPARK-1.0-SNAPSHOT.jar s3://uhdais/data/C2000-9.mtx 0 34.209.44.36 false 128 0

spark-submit --deploy-mode cluster --class mwcp.spark.MWCP --executor-memory 8G --driver-memory 5G --executor-cores 8 --num-executors 8 s3://qiansapp/MWCP_SPARK-1.0-SNAPSHOT.jar s3://uhdais/data/soc-orkut-dir.edges 0 34.209.44.36 false 128 0

spark-submit --deploy-mode cluster --class mwcp.spark.MWCP --executor-memory 8G --driver-memory 5G --executor-cores 8 --num-executors 8 s3://uhdaisqian/app/MWCP_SPARK-1.0-SNAPSHOT.jar s3://uhdais/output/soc-orkut-dir-4 0 34.209.44.36 false 128 0

spark-submit --deploy-mode cluster --class mwcp.spark.MWCP --executor-memory 10G --driver-memory 5G --executor-cores 8 --num-executors 8 s3://uhdaisqian/app/MWCP_SPARK-1.0-SNAPSHOT.jar s3://uhdais/data/soc-orkut-dir.edges 0 34.209.44.36 false 64 0

spark-submit --deploy-mode cluster --class mwcp.spark.MWCP --executor-memory 8G --driver-memory 5G --executor-cores 8 --num-executors 8 s3://uhdaisqian/app/MWCP_SPARK-1.0-
SNAPSHOT.jar s3://uhdais/output/soc-orkut-dir-2 0 34.209.44.36 false 64

spark-submit --deploy-mode cluster --class mwcp.spark.MWCP --executor-memory 8G --driver-memory 5G --executor-cores 8 --num-executors 8 s3://qiansapp/MWCP_SPARK-2.0-SNAPSHOT.jar s3://uhdais/data/web-uk-2005.mtx 0 34.209.44.36 false 64 0

spark-submit --deploy-mode client --class util.GenerateGraphForExperiment --executor-memory 5G --driver-memory 8G --executor-cores 8 --num-executors 8 s3://uhdaisqian/app/MWCP_SPARK-1.0-SNAPSHOT.jar s3://uhdais/data/soc-orkut-dir.edges s3://uhdais/output/soc-orkut-dir-

aws emr add-steps --cluster-id j-ANLBK3N1D0IF --steps Type=Spark,Name="MWCP",ActionOnFailure=CONTINUE,Args=[--class,mwcp.spark.MWCP,--driver-memory,5G,--executor-memory,8G,--executor-cores,8,--num-executors,8,s3://uhdais/applications/MWCP_SPARK-1.0-SNAPSHOT.jar,s3://uhdais/data/ca-hollywood-2009.mtx,0,18.236.223.233,false,64,0]

spark-submit --deploy-mode cluster --class mwcp.spark.MWCP --executor-memory 8G --driver-memory 5G --executor-cores 8 --num-executors 8 s3://uhdaisqian/app/MWCP_SPARK-1.0-SNAPSHOT.jar s3://uhdais/data/ca-hollywood-2009.mtx 0 34.209.44.36 false 64 0

spark-submit --deploy-mode cluster --class mwcp.spark.MWCP --executor-memory 8G --driver-memory 5G --executor-cores 8 --num-executors 8 s3://uhdaisqian/app/MWCP_SPARK-1.0-SNAPSHOT.jar s3://uhdais/data/bio-human-gene1 0 34.209.44.36 false 64 0



s3://uhdaisqian/app/MWCP_SPARK-1.0-SNAPSHOT.jar

--class mwcp.spark.MWCP
--executor-memory 8G
--driver-memory 5G
--executor-cores 8
--num-executors 8

s3://uhdais/output/soc-orkut-dir-4
0
34.209.44.36
false
128
0

——look at the logs of application——

yarn logs -applicationId 

cd /mnt/var/log/hadoop/steps/s-
less controller
less syslog
less stderr
less stdout

—cancel a stel—
yarn application -kill application_1527472433334_0006


—local:
spark-submit --class mwcp.spark.MWCP --master local --deploy-mode client --num-executors 6 --executor-cores 2 --executor-memory 1g build/libs/MWCP_SPARK-1.0-SNAPSHOT.jar data/web-uk-2005.mtx 0 localhost



——start zookeeper———

ssh -i qiansKey.pem hadoop@ec2-34-209-44-36.us-west-2.compute.amazonaws.com
cd /usr/lib/zookeeper/conf
sudo vi zoo.cfg

maxClientCnxns=90

sudo /usr/lib/zookeeper/bin/zkServer.sh start






