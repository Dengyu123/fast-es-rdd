package com.paic.dpp.test;

import com.paic.dpp.fastsync.ESClient;
import com.paic.dpp.fastsync.ESThread;
import com.paic.dpp.pojo.ElasticInformation;
import com.paic.dpp.pojo.IndexInformation;
import com.paic.dpp.util.HdfsUtil;

/**
 * @author dengyu
 * @Function:
 * @date 2020/06/02
 */
public class CreateIndexTest {
    public static void main(String[] args) throws Exception {
        if(args.length!=2){
            System.out.println("Usage: <index-path> <index-name>");
            System.exit(1);
        }
        String indexPath = args[0];
        String indexName = args[1];
        ElasticInformation elasticInformation = init();
        IndexInformation indexInformation = IndexInformation.getIndexInfoFromIndexFile(indexPath, indexName, false, false,HdfsUtil.getConf());
        ESClient esClient = new ESClient(indexInformation,elasticInformation);
//        esClient.createNewIndex();
//        esClient.multiWrite();

    }


    public static ElasticInformation init() throws Exception {
        String localPath = "/Users/dengyu/Documents/hadoop/lucene-data";
        ESThread esThread = new ESThread(localPath);
        esThread.init(HdfsUtil.getConf());
        return esThread.getElasticInformation();
    }
}
