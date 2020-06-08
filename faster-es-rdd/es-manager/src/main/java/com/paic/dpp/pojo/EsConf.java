package com.paic.dpp.pojo;

import com.paic.dpp.anno.ConfigurationProperties;

/**
 * @author dengyu
 * @Function:
 * @date 2020/06/03
 */
@ConfigurationProperties(prefix = "es")
public class EsConf {
    private String zipPath;
    private String version;
    private String clusterNamePrefix;
    private String timeout;

    public String getZipPath() {
        return zipPath;
    }

    public void setZipPath(String zipPath) {
        this.zipPath = zipPath;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getClusterNamePrefix() {
        return clusterNamePrefix;
    }

    public void setClusterNamePrefix(String clusterNamePrefix) {
        this.clusterNamePrefix = clusterNamePrefix;
    }

    public String getTimeout() {
        return timeout;
    }

    public void setTimeout(String timeout) {
        this.timeout = timeout;
    }
}
