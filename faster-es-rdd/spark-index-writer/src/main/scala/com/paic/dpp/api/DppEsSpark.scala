package com.paic.dpp.api

import com.paic.dpp.pojo.IndexInformation
import com.paic.dpp.util.HdfsUtil
import com.paic.dpp.writer.{FastEsWriter}
import org.apache.spark.rdd.RDD

/**
  * @Function:
  * @author dengyu
  * @date 2020/06/03
  */
object DppEsSpark {

  def saveToEs(rdd:RDD[_], indexInfomation: IndexInformation):Unit = {
    rdd.sparkContext.getConf.registerKryoClasses(
      Array(classOf[FastEsWriter[_]]))
    rdd.sparkContext.runJob(rdd,new FastEsWriter(indexInfomation).write _)
  }

}
