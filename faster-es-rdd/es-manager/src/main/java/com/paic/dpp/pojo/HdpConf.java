package com.paic.dpp.pojo;

import com.paic.dpp.anno.ConfigurationProperties;
import org.apache.hadoop.conf.Configuration;

/**
 * @author dengyu
 * @Function:
 * @date 2020/06/02
 */
@ConfigurationProperties(prefix = "hdp")
public class HdpConf {
    private String defaultFs;

    public String getDefaultFs() {
        return defaultFs;
    }

    public void setDefaultFs(String defaultFs) {
        this.defaultFs = defaultFs;
    }

    @Override
    public String toString() {
        return "HdpConf{" +
                "defaultFs='" + defaultFs + '\'' +
                '}';
    }
}
