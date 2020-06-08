package com.paic.dpp.test

import com.alibaba.fastjson.JSONObject
import com.paic.dpp.api.DppEsSpark
import com.paic.dpp.pojo.IndexInformation
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * @Function:
  * @author dengyu
  * @date 2020/06/03
  */
object SparkEsTest {

  def main(args: Array[String]): Unit = {
    val indexName = args(0)
    val indexFile = args(1)
    val conf = new Configuration()
    conf.set("fs.defaultFs","hdfs://master:9000")
    val inf = IndexInformation.getIndexInfoFromIndexFile(indexFile,indexName,false,false,conf)
    val spark = SparkSession.builder()
      .master("local")
      .config("spark.es.lucene.dir","hdfs://master:9000/es/spark-work-place/"+indexName )
      .config("spark.sql.warehouse.dir","hdfs://master:9000/warehose")
      .config("hive.metastore.uris","thrift://master:9083")
      .config("hive.metastore.local","false")
      .config("beeline.hs2.connect.user","root")
      .config("beeline.hs2.connect.password","123456")
      .enableHiveSupport()
      .getOrCreate()
    val data = spark.sql("select * from fast.user_dimension")
      .rdd.map(x => {val json = new JSONObject()
      val fields = x.schema.fields
      fields.foreach(field => {
        json.put(field.name, x.getAs(field.name))
      })
      (null, json)
    })
    DppEsSpark.saveToEs(data,inf)
  }

}
