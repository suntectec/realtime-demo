package com.sands.realtime.ods.function;

import com.alibaba.fastjson2.JSON;

import com.sands.realtime.common.bean.ods.SqlServerOrdersBean;
import com.sands.realtime.common.bean.ods.SqlServerOrdersInfo;
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
public class OrdersProcessFunction extends ProcessFunction<SqlServerOrdersBean, SqlServerOrdersInfo> {

    @Override
    public void processElement(SqlServerOrdersBean sqlServerOrdersBean, ProcessFunction<SqlServerOrdersBean, SqlServerOrdersInfo>.Context context, Collector<SqlServerOrdersInfo> collector) throws Exception {

        // 添加空值检查
        if (sqlServerOrdersBean == null || sqlServerOrdersBean.getAfter() == null) {
            // 可以选择记录日志、跳过或者处理异常数据
            System.err.println("Warning: Received null data or info is null: " + sqlServerOrdersBean);
            return; // 跳过这条记录
        }
        SqlServerOrdersInfo after = sqlServerOrdersBean.getAfter();
        after.set_row_kind(sqlServerOrdersBean.getOp());

        // System TimeStamp, Long -> DateTime
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String procTime = dateTimeFormatter.format(Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault()));
        after.set_proc_time(procTime);

        collector.collect(sqlServerOrdersBean.getAfter());

    }

}
