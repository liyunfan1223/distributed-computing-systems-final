该仓库记录了分布式计算系统期末实验设计中配套使用的代码、负载生成和运行脚本脚本，并介绍如何使用。

## 项目布局
```
├─input             负载数据
├─out               
│  └─artifacts
│      └─DCS-1.0    jar包  
├─scripts           数据生成和任务提交的脚本
└─src
```
## 环境配置
hadoop版本: 2.10.1						

spark版本:2.4.7

jdk版本: 1.8.0_171
## 编译打包

可跳过，直接使用已打包好的out/artifacts/DCS_1_0/DCS-1.0.jar

```shell
mvn clean package
mv target/DCS-1.0.jar out/artifacts/DCS_1_0/DCS-1.0.jar
```

## 运行脚本
使用脚本前需要启动HDFS和spark，配置hadoop和spark的环境变量，并将主节点设置为ecnu01

### 生成负载并上传至HDFS
```
scripts/gen_and_upload.sh
```

### 实验一
```
scripts/submit_shuffle_group.sh
scripts/submit_shuffle_reduce.sh
```

### 实验二
```
scripts/submit_spill_partition2.sh
scripts/submit_spill_partition16.sh
```

### 实验三
```
scripts/submit_skewed_basic.sh
scripts/submit_skewed_salting.sh
```