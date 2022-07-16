package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * @Description 操作kafka的工具类
 * @Author mei
 * @Data 2022/7/1321:20
 */
public class MyKafkaUtil {
    public static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    //获取消费者对象
    public  static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        //注意：这种方式使用SimpleStringSchema进行反序列化时，如果kafka主题中消息为null，会报错
        //FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }

            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                if (consumerRecord != null && consumerRecord.value() != null) {
                    return new String(consumerRecord.value());
                }
                return null;
            }
        }, properties);
        return kafkaConsumer;
    }
}
