package com.sands.realtime.ods.function;

import com.alibaba.fastjson2.JSON;
import com.sands.realtime.common.bean.ods.SqlserverOrdersBean;
import com.sands.realtime.common.bean.ods.SqlserverOrdersInfo;
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
public class OrdersProcessFunction extends ProcessFunction<SqlserverOrdersBean, String> {

    @Override
    public void processElement(SqlserverOrdersBean sqlserverOrdersBean, ProcessFunction<SqlserverOrdersBean, String>.Context context, Collector<String> collector) throws Exception {

        sqlserverOrdersBean.getAfter().set_rowKind(sqlserverOrdersBean.getOp());
        // System TimeStamp, Long -> DateTime
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String procTime = dateTimeFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault()));
        sqlserverOrdersBean.getAfter().set_procTime(procTime);

        collector.collect(JSON.toJSONString(sqlserverOrdersBean.getAfter()));

    }

}
