package Conf;

import org.apache.hadoop.conf.Configuration;

/**
 * @ClassName: SingleConfiguration
 * @Author: HongLiang
 * @Description: TODO
 * @Date: Create in 23:01 2021/1/28
 * @Version: 1.0
 * @Modified By:HongLiang
 */
public class SingleConfiguration {
    private static Configuration configuration = new Configuration();
    public Configuration getConfiguration() {
        return configuration;
    }
}
