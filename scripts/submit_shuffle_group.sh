spark-submit \
--master spark://ecnu01:7077 \
--class cn.edu.ecnu.distributed.aggregate.GroupByKeyTest \
./out/artifacts/DCS_1_0/DCS-1.0.jar \
hdfs://ecnu01:9000/user/ubuntu/input/data_lab1.txt \
hdfs://ecnu01:9000/user/ubuntu/output