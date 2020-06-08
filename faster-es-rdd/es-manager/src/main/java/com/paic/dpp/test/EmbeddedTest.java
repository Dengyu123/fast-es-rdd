package com.paic.dpp.test;

import com.paic.dpp.factory.PojoFactory;
import com.paic.dpp.fastsync.ESThread;
import com.paic.dpp.pojo.EsConf;
import com.paic.dpp.pojo.HdpConf;
import com.paic.dpp.util.HdfsUtil;
import com.paic.embeded.EmbeddedElastic;
import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static com.paic.embeded.PopularProperties.*;

/**
 * @author dengyu
 * @Function:
 * @date 2020/06/02
 */
public class EmbeddedTest {

    public static void main(String[] args) throws Exception {
        String localPath = "/Users/dengyu/Documents/hadoop/lucene-data";
        ESThread esThread = new ESThread(localPath);
        esThread.init(HdfsUtil.getConf());
//        int count = 0;
//        while (true){
//            Thread.sleep(20000);
//            System.out.println("sleep .... ");
//            count ++;
//            if(count == 5){
//                esThread.stop();
//                break;
//            }
//        }



    }

}
