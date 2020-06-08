package com.paic.dpp.test;

import com.alibaba.fastjson.JSONObject;
import com.paic.dpp.fastsync.ESClient;
import com.paic.dpp.util.FileUtil;
import com.paic.dpp.util.HdfsUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author dengyu
 * @Function:
 * @date 2020/06/02
 */
public class WriteIndexTest {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","hadoop");
        if(args.length!=3){
            System.out.println("Usage: <index-path> <data-path> <index-name>");
            System.exit(1);
        }
        String indexPath = args[0];
        String dataPath = args[1];
        String indexName = args[2];
        String localPath = "/Users/dengyu/Documents/hadoop/lucene-data";
//        ESThread esThread = new ESThread(localPath);
//        esThread.init(HdfsUtil.getConf());
//        ElasticInformation elasticInformation = esThread.getElasticInformation();
//        IndexInformation indexInfomation = IndexInformation.getIndexInfoFromIndexFile(indexPath,indexName,false,false);
//        ESClient esClient = new ESClient(indexInfomation,elasticInformation);
//        List<ESClient.DocData> docDataList = loadDataFromJson(dataPath);
//        esClient.multiWrite(loadDataFromJson(dataPath));
//        esClient.putSetting("index.translog.retention.size", "8mb");
//        if(!esClient.refresh()) {
//            System.out.println("refresh fail");
//        }
//        if(!esClient.flush()){
//            System.out.println("flush fail");
//        }
//        esClient.forceMerge();
//        esThread.stop();
        //打包文件
        String entry = localPath+"/data";
        String archive = FileUtil.archive(entry);
        String path = FileUtil.compressArchive(archive);
        HdfsUtil.mkdir("hdfs://master:9000/es/work/01",HdfsUtil.getConf());
        HdfsUtil.uploadFile(path,"hdfs://master:9000/es/work/01",HdfsUtil.getConf());

    }

    private static List<ESClient.DocData> loadDataFromJson(String dataPath) throws IOException {
        List<String> lines = FileUtils.readLines(new File(dataPath));
        List<ESClient.DocData> docDataList = new ArrayList<>();
        for (String line : lines) {
            JSONObject jsonObject = JSONObject.parseObject(line);
            ESClient.DocData docData  = new ESClient.DocData();
            docData.key = null;
            docData.data = jsonObject;
            docDataList.add(docData);
        }
        return docDataList;
    }



}
