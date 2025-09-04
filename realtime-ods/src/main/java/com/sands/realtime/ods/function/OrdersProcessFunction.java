package com.sands.realtime.ods.function;

import com.alibaba.fastjson2.JSON;
import com.sands.realtime.common.bean.ods.SqlserverOrdersInfo;
import com.sands.realtime.common.bean.ods.SqlserverOrdersInputBean;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 获取 debezium 中 after 为表数据，添加额外信息构造表数据
 *
 * @author Jagger
 * @since 2025/8/26 14:40
 */
public class OrdersProcessFunction extends ProcessFunction<SqlserverOrdersInputBean, String> {

    @Override
    public void processElement(SqlserverOrdersInputBean sqlserverOrdersInputBean, ProcessFunction<SqlserverOrdersInputBean, String>.Context context, Collector<String> collector) throws Exception {

        SqlserverOrdersInfo sqlserverOrdersInfo = sqlserverOrdersInputBean.getSqlserverOrdersInfo();
        sqlserverOrdersInfo.set_rowKind(sqlserverOrdersInputBean.getOp());
        // Long -> DateTime
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String procTime = dateTimeFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault()));
        sqlserverOrdersInfo.set_procTime(procTime);
        collector.collect(JSON.toJSONString(sqlserverOrdersInfo));

    }

}
