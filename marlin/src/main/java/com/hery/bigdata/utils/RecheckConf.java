package com.hery.bigdata.utils;

import com.hery.bigdata.constant.BaseConstants;
import org.apache.commons.configuration.CombinedConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.tree.OverrideCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

/**
 * <p>用于加载配置文件和缓存一些静态数据，比如衍生变量配置</p>
 *
 * @author wangpeng
 * @version 1.0.0
 */
public class RecheckConf {

    private static final Logger logger = LoggerFactory.getLogger(RecheckConf.class);

    private static CombinedConfiguration props = null;
    static {
        props = new CombinedConfiguration();
        props.setNodeCombiner(new OverrideCombiner());
        try {
             File file = new File(BaseConstants.HYSAAS_PROD_PROP_PATH);
            final PropertiesConfiguration prodProps = new PropertiesConfiguration(file);
            logger.info("load prod config success");
            props.addConfiguration(prodProps);
        } catch (ConfigurationException e) {
            logger.warn("No prod property file");
        }

        try {
            URL defaultPropUrl = RecheckConf.class.getResource(BaseConstants.HYSAAS_LOCAL_PROP_PATH);
//            RecheckConf.class.getClassLoader().getResourceAsStream(BaseConstants.HYSAAS_LOCAL_PROP_PATH);
            final PropertiesConfiguration defaultProps = new PropertiesConfiguration(defaultPropUrl);
            logger.info("load default config: {}", defaultPropUrl);
            props.addConfiguration(defaultProps);
        } catch (ConfigurationException e) {
            throw new RuntimeException("No default app property file");
        }
    }
    public static String getString(String key) {
        return props.getString(key);
    }

    public static void main(String[] args) {
        System.out.println(RecheckConf.getString("hive.site"));
    }
}
