package tsinghua;

/*
  author: liurui
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tsinghua.conf.Config;
import tsinghua.conf.ConfigDescriptor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App 
{
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main( String[] args )
    {

        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
        logger.info("thread num : {}", config.CLIENT_NUMBER);
        for(int i = 0; i < config.CLIENT_NUMBER; i++) {
            executorService.submit(new KProducer(config.TOPIC, i));
        }
        executorService.shutdown();
    }
}
