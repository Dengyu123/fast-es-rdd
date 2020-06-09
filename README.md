## 从HADOOP/HIVE实现快速导数入ES

### 说明

实现完整功能，需要将这些步骤拼装：

1.spark读取数据后输出lucene文件，并上传hdfs

2.从hdfs拉取对应index的lucene文件夹，scp到es集群

3.es集群加载数据



### 主程序

参考滴滴fast-loader，改用Spark实现部分功能

Spark api：

spark-index-writer工程下：com.paic.dpp.api.DppEsSpark

例子可参考
[com.paic.dpp.test.SparkEsTest](https://github.com/Dengyu123/fast-es-rdd/blob/master/faster-es-rdd/spark-index-writer/src/main/scala/com/paic/dpp/test/SparkEsTest.scala)


### index建表格式：

参考spark-index-writer/batch/dyfirstindex.index

$1: nested标识，0表示第一层字段，非0表示该字段下的nested字段

$2: field名称

$3: field类型(string,integer,long,date,double...)

$4:分词模式(0:keyword模式，1:text&keyword模式)



### 如何调用DppEsSpark？

1.通过加载indexFile生成IndexInformation实例

```
IndexInformation.getIndexInfoFromIndexFile(indexFile,indexName,false,false,conf)
参数1:indexFile本地地址
参数2:index名称
参数3:是否开启nested标识，false则为普通嵌套
参数4:是否开启自动识别date，建议为false
参数5:hadoop conf
```

2.生成RDD

Rdd为RDD(String,JSONObject)格式传入，第一位为null表示不指定id, 系统会自动分配随机UUID。第二位为数据JSON。将indexInformation（inf）传入，即可生成lucene文件，并压缩上传到hdfs

```
DppEsSpark.saveToEs(data,inf)
```



### 配置项

es-manager工程下fer.properties:

```properties
#配置es安装文件和版本，可根据自己集群的版本更换zip文件，并更换version版本
es.zipPath=hdfs://master:9000/es/elasticsearch-6.1.2.zip  
es.version=6.1.2
#虚拟es实例的单节点的集群名称，可自定义
es.clusterNamePrefix=faster_local_cluster
es.timeout=120
```

spark配置参数需要：spark.es.lucene.dir

```
.config("spark.es.lucene.dir","hdfs://master:9000/es/spark-work-place/"+indexName )
```

hadoop conf需要配置fs.defaultFs

```
conf.set("fs.defaultFs","hdfs://master:9000")
```

### ES集群操作

加载文件操作集群步骤参考：

spark-index-writer/batch/dataLoader.sh



