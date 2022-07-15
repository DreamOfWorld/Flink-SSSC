package com.atguigu.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description 从主流数据中过滤出维度数据
 * @Author mei
 * @Data 2022/7/1520:39
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction() {
    }

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //处理主流业务数据
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

    }

    //处理广播流配置数据
    @Override
    public void processBroadcastElement(String jsonStr, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {


       //为了方便处理，将finkCDC 采集的数据，封装为json对象
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);

        //获取广播状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //货物对配置表的操作类型
        String op = jsonObject.getString("op");
        if ("d".equals(op)){
            //如果对配置表进行的是删除操作，将对应的配置信息从广播状态中删除处理
            TableProcess before = jsonObject.getObject("before", TableProcess.class);
            String sourceTable = before.getSourceTable();
            broadcastState.remove(sourceTable);
        }else {
            //如果对配置表进行的是添加或者是修改操作，键对应的配置信息添加到广播状态或者对广播状态中的配置进行修改
            TableProcess after = jsonObject.getObject("after", TableProcess.class);
            //获取业务数据库表名
            String sourceTable = after.getSourceTable();
            //获取数仓中输出的目的地表名
            String sinkTable = after.getSinkTable();
            //获取数仓中对应的表中的字段
            String sinkColumns = after.getSinkColumns();
            //获取数仓中对应的表的主键
            String sinkPk = after.getSinkPk();
            //获取建表扩展
            String sinkExtend = after.getSinkExtend();

            //将添加或修改后的配置放到广播状态中
            broadcastState.put(sourceTable,after);
        }
    }
}
