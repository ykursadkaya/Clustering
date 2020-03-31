$HADOOP_PREFIX/bin/hdfs dfsadmin -safemode leave

$HADOOP_PREFIX/bin/hdfs dfs -mkdir /
$HADOOP_PREFIX/bin/hdfs dfs -mkdir /user
$HADOOP_PREFIX/bin/hdfs dfs -mkdir /user/clustering

$HADOOP_PREFIX/bin/hdfs dfs -chown -R clustering:clustering /user/clustering
$HADOOP_PREFIX/bin/hdfs dfs -put ./data/*_data.csv /user/clustering

$HADOOP_PREFIX/bin/hdfs dfs -ls /user/clustering
