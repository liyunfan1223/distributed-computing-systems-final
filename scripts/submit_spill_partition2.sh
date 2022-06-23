spark-submit \
--master spark://ecnu01:7077 \
--class cn.edu.ecnu.distributed.join.HashShuffleJoin \
~/myApp/DCS_Final.jar \
hdfs://ecnu01:9000/user/ubuntu/input/data_lab2.txt \
hdfs://ecnu01:9000/user/ubuntu/input/data_lab2.txt \
hdfs://ecnu01:9000/user/ubuntu/output \
2