package tsinghua;

import com.alibaba.fastjson.JSON;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tsinghua.conf.Config;
import tsinghua.conf.ConfigDescriptor;
import tsinghua.conf.Constants;
import tsinghua.function.Function;
import tsinghua.function.FunctionParam;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class KProducer extends Thread {
    private String topic;
    private int deviceIndex;
    private final String TENANT = "tenant";
    private final String USER = "user";
    private final String TSTABLE = "tstable";
    private final String TIMESTAMP = "timestamp";
    private final String TAGS = "tags";
    private final String SENSORS = "sensors";
    private Map<String, String> tagMap ;
    private Config config;
    private static Logger logger = LoggerFactory.getLogger(KProducer.class);

    public KProducer(String topic, Map<String, String> tagMap) {
        super();
        this.topic = topic;
        this.tagMap = tagMap;
        config = ConfigDescriptor.getInstance().getConfig();
    }

    public KProducer(String topic, int deviceIndex) {
        super();
        this.topic = topic;
        this.deviceIndex = deviceIndex;
        tagMap = new HashMap<String, String>();
        tagMap.put("device", "d_" + deviceIndex);
        config = ConfigDescriptor.getInstance().getConfig();
    }

    @Override
    public void run() {
        Producer<Integer, String> producer = createProducer();
        for(long i=0;i<config.LOOP;i++) {
            String insertionJSON = generateJSON("myTenant", "user1", "performf.group_0", i, 0);
            producer.send(new KeyedMessage<Integer, String>(topic, insertionJSON));
            try {
                TimeUnit.MILLISECONDS.sleep(config.POINT_STEP);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
        producer.close();
    }


//    { 
//        "tenant":"myTenant", 
//        "user":"user1", 
//        "tstable":"group_0",
//            "timestamp":"2018-01-01T00:00:00.000+08:00",
//            "tags":{
//        "device":"d_0",
//                "part":"p_0"
//    }, 
//        "sensors":{ 
//        "s_0":5, 
//        "s_1":123, 
//        "s_2":10.34, 
//        "s_3":23 
//    } 
//    }

    /**
     * 日期格式转换yyyy-MM-dd'T'HH:mm:ss.SSSXXX  (yyyy-MM-dd'T'HH:mm:ss.SSSZ) TO  yyyy-MM-dd HH:mm:ss
     * @throws ParseException
     */
    public String dealDateFormat(long timestamp) throws ParseException {
        Date date = new Date(timestamp);
        DateFormat df = new SimpleDateFormat(config.TIME_FORMAT);//yyyy-MM-dd'T'HH:mm:ss.SSSZ
        return df.format(date);
    }

    private String generateJSON(String tenant, String user, String tsTable, long loopIndex, long dataIndex) {
        Map<String, Object> insertMap = new HashMap<String, Object>();
        Map<String, Object> sensorMap = new HashMap<String, Object>();
        insertMap.put(TENANT, tenant);
        insertMap.put(USER, user);
        insertMap.put(TSTABLE, tsTable);
        //long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (loopIndex * config.CACHE_NUM + dataIndex);
        long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * loopIndex ;
        if(config.TIME_FORMAT.equals("long")){
            insertMap.put(TIMESTAMP, currentTime);
        } else {
            try {
                String timeString = dealDateFormat(currentTime);
                insertMap.put(TIMESTAMP, timeString);
            } catch (ParseException e) {
                logger.error("时间戳格式转换失败");
                e.printStackTrace();
            }
        }
        insertMap.put(TAGS, tagMap);

        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            Number value = Function.getValueByFuntionidAndParam(param, currentTime);
            sensorMap.put(sensor, value);
        }
        insertMap.put(SENSORS, sensorMap);
        return JSON.toJSONString(insertMap);
    }

    private Producer<Integer, String> createProducer() {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", config.host + ":" + config.port);
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", config.BROKER_LIST);// 声明kafka broker
        return new Producer<Integer, String>(new ProducerConfig(properties));
    }

    public static void main(String[] args) {

        new KProducer("test", 1).start();
    }
}
