package com.hery.bigdata.conf;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.tree.OverrideCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.List;

/**
 * Created by houyongheng on 2019/7/23.
 * 配置文件维护
 */
public class MarlinProperty {
    private final static Logger logger = LoggerFactory.getLogger(MarlinProperty.class);

    private static CombinedConfiguration props = null;

    static {
        props = new CombinedConfiguration();
        props.setNodeCombiner(new OverrideCombiner());

        try {
            File file = new File("/data/bgservice/sparkapp/marlin/prod/marlin-prod.properties");
            final PropertiesConfiguration prodProps = new PropertiesConfiguration(file);
            logger.info("load prod config: {}", file.getPath());
            props.addConfiguration(prodProps);
        } catch (ConfigurationException e) {
            logger.warn("No prod property file");
        }

        try {
            URL defaultPropUrl = MarlinProperty.class.getResource("/marlin-local.properties");
            final PropertiesConfiguration defaultProps = new PropertiesConfiguration(defaultPropUrl);
            logger.info("load default config: {}", defaultPropUrl);
            props.addConfiguration(defaultProps);
        } catch (ConfigurationException e) {
            throw new RuntimeException("No default app property file");
        }
    }

    public String getString(String key) {
        return props.getString(key);
    }

    public Boolean getBoolean(String key){
        return props.getBoolean(key);
    }

    public int getInteger(String key) {
        return props.getInt(key);
    }


    public AbstractConfiguration getProperties() {
        return props;
    }
}
