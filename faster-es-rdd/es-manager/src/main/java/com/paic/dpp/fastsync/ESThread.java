package com.paic.dpp.fastsync;

import com.paic.dpp.factory.PojoFactory;
import com.paic.dpp.pojo.ElasticInformation;
import com.paic.dpp.pojo.EsConf;
import com.paic.dpp.util.HdfsUtil;
import com.paic.embeded.EmbeddedElastic;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.paic.dpp.util.PortUtils.choicePort;
import static com.paic.embeded.PopularProperties.*;

/**
 * @author dengyu
 * @Function:
 * @date 2020/06/02
 * 独立ES进程
 * 领域：ES进程，拟集群节点
 */
public class ESThread {

    private String localWorkPathAbs;
    private EmbeddedElastic elastic;
    private ElasticInformation elasticInformation;
    private Logger logger = LoggerFactory.getLogger(ESThread.class);
    private static EsConf esConf = (EsConf) PojoFactory.getServiceBean("esConf");
    public ESThread(String localWorkPathAbs){
        this.localWorkPathAbs = localWorkPathAbs;
    }

    public void init(Configuration configuration) throws Exception {
        //clear working directory
        HdfsUtil.deleteDir(configuration,localWorkPathAbs);
        start(configuration);
    }

    /**
     * 异步 构建ES单例
     * @throws Exception
     */
    private void start(Configuration configuration) throws Exception {
        List<Integer> ports = choicePort();

        elastic = EmbeddedElastic.builder()
                .withElasticVersion(esConf.getVersion())
                .withSetting(HTTP_PORT,ports.get(0))
                .withSetting(TRANSPORT_TCP_PORT,ports.get(1))
                .withSetting(CLUSTER_NAME,esConf.getClusterNamePrefix()+ports.get(0))
                .withEsJavaOpts("-Xms128m -Xmx512m")
                .withSetting("discovery.type", "single-node")
                .withSetting(PATH_DATA,localWorkPathAbs+"/data")
                .withInstallationDirectory(new File(localWorkPathAbs + "/es"))
                .withDownloadDirectory(new File(localWorkPathAbs + "/download"))
                .withInResourceLocation(HdfsUtil.download2Local(esConf.getZipPath(),configuration))
                .withStartTimeout(Integer.parseInt(esConf.getTimeout()), TimeUnit.SECONDS)
                .build().start();
        elasticInformation = new ElasticInformation("127.0.0.1",ports.get(1),esConf.getClusterNamePrefix()+ports.get(0));
        logger.info("Start es thread of:"+ports.get(0));
    }

    public ElasticInformation getElasticInformation() {
        return elasticInformation;
    }

    /* 停止ES进程 */
    public void stop() {
        logger.info("close es node start");
        try {
            if (this.elastic!= null) {
                elastic.stop();
            }
        } catch (Throwable t) {
            logger.info("close es error", t);
        }
        logger.info("close es node stop");
    }

}
