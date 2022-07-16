package com.atguigu.gmall.realtime.app.dim;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @Description 维度处理
 * @Author mei
 * @Data 2022/7/1319:58
 *
 * @Note 执行流程
 *  1.运行模拟生成业务数据的jar包
 *  2.将生成的业务数据保存在业务数据库中
 *  3.binlog会记录业务数据库的变化
 *  4.Maxwell从binlog中读取变化数据并且将数据封装为json格式字符串发送给kafka的topic_db主题
 *  5.DimApp从topic_db主题中读取数据进行输出
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2.检查点相关设置
        //2.1 开启检查点
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 取消job后检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //2.5 设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        //2,7指定操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        //TODO 3.从kafka的topic_db主题中读取业务数据
        //3,1声明消费的主题和消费者组
        String topic = "topic_db";
        String groupId = "dim_sink_group";
        //3.2创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //3.3消费数据 封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);
        //TODO 4.对读取的数据进行转换 jsonStr -> jsonObj
        //匿名内部类方式
        SingleOutputStreamOperator<JSONObject> jsonObjDS1 = kafkaStrDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                return jsonObject;
            }
        });
        //lambda表达式方式
        //SingleOutputStreamOperator<JSONObject> jsonObjDS2 = kafkaStrDS.map(jsonStr -> JSONObject.parseObject(jsonStr));
        //方法的默认调用
//        SingleOutputStreamOperator<JSONObject> jsonObjDS3 = kafkaStrDS.map(JSON::parseObject);

        //TODO 5.对读取的主流业务数据进行简单的ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS1.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                try {
                    jsonObject.getJSONObject("data");
                    if (jsonObject.getString("type").equals("bootstrap-start")
                            || jsonObject.getString("type").equals("bootstrap-complete")) {
                        return false;
                    }
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }
        });
        //filterDS.print("filterDS >>> ");
        //TODO 6.使用FlinkCDC读取配置表数据-配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall0106_config")
                .tableList("gmall0106_config.table_process")
                .username("root")
                .password("my19970929")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MYSQL Source");
//        mysqlDS.print(" >>>>>> ");
        //TODO 7.将配置流进行广播--广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("mapStateDescriptor",String.class,TableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlDS.broadcast(mapStateDescriptor);
        //TODO 8.将业务主流和配置广播流进行关联--connect
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);
        //TODO 9.对关联之后的数据进行处理--process 从业务流中将维度过滤出来
        SingleOutputStreamOperator<JSONObject> dimDS = connectDS.process(new TableProcessFunction(mapStateDescriptor));
        //TODO 10.将处理得到的维度数据写到Phoenix表中
        env.execute();
    }
}
