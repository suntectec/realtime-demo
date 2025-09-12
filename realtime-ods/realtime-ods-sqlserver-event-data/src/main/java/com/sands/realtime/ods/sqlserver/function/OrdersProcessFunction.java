package com.sands.realtime.ods.sqlserver.function;

import com.sands.realtime.common.bean.ods.SqlServerOrdersEventData;
import com.sands.realtime.common.bean.ods.SqlServerOrdersAfterInfo;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * 获取 debezium 中 after 为表数据，添加额外信息构造表数据
 *
 * @author Jagger
 * @since 2025/8/26 14:40
 */
public class OrdersProcessFunction extends ProcessFunction<SqlServerOrdersEventData, SqlServerOrdersAfterInfo> {

    @Override
    public void processElement(SqlServerOrdersEventData sqlServerOrdersBean, ProcessFunction<SqlServerOrdersEventData, SqlServerOrdersAfterInfo>.Context context, Collector<SqlServerOrdersAfterInfo> collector) throws Exception {

        // 添加空值检查
        if (sqlServerOrdersBean == null || sqlServerOrdersBean.getAfter() == null) {
            // 可以选择记录日志、跳过或者处理异常数据
            System.err.println("Warning: Received null data or info is null: " + sqlServerOrdersBean);
            return; // 跳过这条记录
        }
        SqlServerOrdersAfterInfo after = sqlServerOrdersBean.getAfter();
        after.set_row_kind(sqlServerOrdersBean.getOp());

        after.set_source_time(sqlServerOrdersBean.getSourceTime());
        after.set_ingestion_time(sqlServerOrdersBean.getIngestionTime());
        after.set_process_time(new Date());

        collector.collect(sqlServerOrdersBean.getAfter());

    }

}
