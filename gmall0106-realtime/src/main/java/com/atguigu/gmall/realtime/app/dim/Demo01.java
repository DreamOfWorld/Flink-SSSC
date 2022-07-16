package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.Mytest;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * @Description
 * @Author mei
 * @Data 2022/7/1516:31
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        env.enableCheckpointing(3 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3 * 1000L);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME","atguigu");

        String topic = "topic_db";
        String groupId = "dim_app_sink";
        FlinkKafkaConsumer<String> flinkKafkaConsumer = Mytest.getFlinkKafkaConsumer(topic, groupId);
        DataStreamSource<String> flinkKafkaConsumerDS = env.addSource(flinkKafkaConsumer);

        SingleOutputStreamOperator<JSONObject> map1 = flinkKafkaConsumerDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONObject.parseObject(s);
            }
        });
        SingleOutputStreamOperator<JSONObject> map2 = flinkKafkaConsumerDS.map(fkc -> JSONObject.parseObject(fkc));
        SingleOutputStreamOperator<JSONObject> map3 = flinkKafkaConsumerDS.map(JSONObject::parseObject);

        SingleOutputStreamOperator<JSONObject> filterDS = map1.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                try {
                    jsonObject.getJSONObject("data");
                    if (jsonObject.getString("type").equals("bootstrap-start") || jsonObject.getString("type").equals("bootstrap-complete")) {
                        return false;
                    }
                    return true;
                } catch (Exception e) {
                    return false;
                }

            }
        });
//        filterDS.print();
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall0106_config") // set captured database
                .tableList("gmall0106_config.table_process") // set captured table
                .username("root")
                .password("my19970929")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        DataStreamSource<String> mySQL_sourceDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

//        mySQL_sourceDS.print(">>>");

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mySQL_sourceDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);

        SingleOutputStreamOperator<JSONObject> dimDS = connectDS.process(new BroadcastProcessFunction<JSONObject, String, JSONObject>() {
            @Override
            public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

            }

            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

            }
        });
        env.execute();
    }
}
