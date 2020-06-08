package com.paic.dpp.pojo;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.paic.embeded.PopularProperties.*;
/**
 * @author dengyu
 * @Function:
 * @date 2020/06/03
 */
public class ElasticInformation implements Serializable {
    private Logger logger  = LoggerFactory.getLogger(ElasticInformation.class);
    private TransportClient client;
    private String ip;
    private int tcpPort;
    private String clusterName;

    public ElasticInformation(String ip, int tcpPort, String clusterName) {
        this.ip = ip;
        this.tcpPort = tcpPort;
        this.clusterName = clusterName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public TransportClient getClient() {
        return client;
    }

    public int getTcpPort() {
        return tcpPort;
    }

    /**
     * init connect
     * @throws UnknownHostException
     */
    public void initConnection() throws UnknownHostException {
        System.setProperty("es.set.netty.runtime.available.processors","false");
        Settings settings = Settings.builder()
                .put(CLUSTER_NAME,clusterName)
                .put("client.transport.sniff",true)
//                .put("cluster.routing.allocation.disk.watermark.flood_stage", "10mb")
//                .put("cluster.routing.allocation.disk.watermark.high", "20mb")
//                .put("cluster.routing.allocation.disk.watermark.low", "30mb")
                .build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName(ip), tcpPort));
        logger.info("Start connection to local es single cluster:"+clusterName);
    }

    /**
     * close connect
     */
    public void closeConnection(){
        if(client !=null) {
            logger.info("Close connection to local es");
            client.close();
        }else{
            logger.warn("Connection client is null!");
        }

    }
}
