package com.paic.dpp.writer

import java.io.File
import java.util

import com.alibaba.fastjson.JSONObject
import com.paic.dpp.fastsync.ESClient.DocData
import com.paic.dpp.fastsync.{ESClient, ESThread}
import com.paic.dpp.pojo.IndexInformation
import com.paic.dpp.util.{FileUtil, HdfsUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkEnv, TaskContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
/**
  * @Function:
  * @author dengyu
  * @date 2020/06/03
  */
private[dpp] class FastEsWriter[T:ClassTag](
                                            val indexInfomation: IndexInformation) extends Serializable {

  val logger:Logger = LoggerFactory.getLogger(this.getClass)
  var workpath :String = ""
  protected def init(conf:Configuration): ESThread = {
    val localPath = "./lucene-data"
    val lucenePath = new File(localPath)
    if(!lucenePath.exists()) lucenePath.mkdir() else {
      lucenePath.deleteOnExit()
      lucenePath.mkdir()
    }
    workpath = lucenePath.getAbsolutePath
    val esThread = new ESThread(workpath)
    esThread.init(conf)
    return esThread
  }


  def write(taskContext: TaskContext,data:Iterator[_]):Unit = {

    //create index
    val workDir = SparkEnv.get.conf.get("spark.es.lucene.dir")
    val re = """(hdfs://[^/]+)(.*)""".r
    val re(fs,wp) = workDir
    val conf = new Configuration()
    conf.set("fs.defaultFs",fs)
    val esThread = init(conf)
    val elasticInformation = esThread.getElasticInformation
    val esClient = new ESClient(indexInfomation, elasticInformation)
    try{
      esClient.createNewIndex()
    }catch {
      case e:Exception => {
        e.printStackTrace()
        logger.error("create index error, please check message")
        return
      }
    }

    //write data to index
    val transData = data.map(x => {
      val (k, v) = x
      val docData = new DocData
      docData.key = k.asInstanceOf[String]
      docData.data = v.asInstanceOf[JSONObject]
      docData
    })

    logger.info("Prepare data of doc data format is successful")

    try{
//      esClient.multiWrite(transData)
      esClient.multiWriteAsRequest(transData.toList)
    }catch {
      case e:Exception =>{
        e.printStackTrace()
        logger.error("write data failed!!!")
        return
      }
    }

    esClient.putSetting("index.translog.retention.size", "8mb")
    if(!esClient.refresh()) {
      logger.error("refresh index shard fail")
    }
    if(!esClient.flush()){
      logger.error("flush index shard fail")
    }
    esClient.forceMerge()
    esThread.stop()
    // tar file to hdfs
    val entry = workpath+"/data"
    val archive = FileUtil.archive(entry)
    val path = FileUtil.compressArchive(archive)
    val nodePath = workDir +"/"+taskContext.taskAttemptId()
    HdfsUtil.mkdir(nodePath, conf)
    HdfsUtil.uploadFile(path,nodePath, conf)

  }

}
