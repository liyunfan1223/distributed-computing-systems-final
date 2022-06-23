python3 scripts/workload_generator.py
hdfs dfs -mkdir -p input
hdfs dfs -put ./input/data_lab1.txt input
hdfs dfs -put ./input/data_lab2.txt input
hdfs dfs -put ./input/data_lab3.txt input