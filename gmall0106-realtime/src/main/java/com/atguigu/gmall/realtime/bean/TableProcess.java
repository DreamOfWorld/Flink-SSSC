package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * @Description 配置表对应实体类
 * @Author mei
 * @Data 2022/7/1514:50
 */
@Data
public class TableProcess {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
