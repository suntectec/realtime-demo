package com.sands.realtime.ods.function;

import com.sands.realtime.common.bean.ods.SqlServerOrdersEventInfo;
import com.sands.realtime.common.bean.ods.SqlServerOrdersEventData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * 获取 debezium 中 after或before 作为表数据，并添加额外信息
 *
 * @author Jagger
 * @since 2025/8/26 14:40
 */
@Slf4j
public class OrdersProcessFunction extends ProcessFunction<SqlServerOrdersEventData, SqlServerOrdersEventInfo> {

    @Override
    public void processElement(SqlServerOrdersEventData sqlServerOrdersEventData, ProcessFunction<SqlServerOrdersEventData, SqlServerOrdersEventInfo>.Context context, Collector<SqlServerOrdersEventInfo> collector) throws Exception {

        // 空值检查
        if (sqlServerOrdersEventData.getAfter() != null) { // 新增
            SqlServerOrdersEventInfo sqlServerOrdersEventInfo = sqlServerOrdersEventData.getAfter();
            sqlServerOrdersEventInfo.set_row_kind(sqlServerOrdersEventData.getOp());

            sqlServerOrdersEventInfo.set_source_time(sqlServerOrdersEventData.getSourceTime());
            sqlServerOrdersEventInfo.set_ingestion_time(sqlServerOrdersEventData.getIngestionTime());
            sqlServerOrdersEventInfo.set_process_time(new Date());

            collector.collect(sqlServerOrdersEventData.getAfter());
        } else if (sqlServerOrdersEventData.getBefore() != null) { // 删除
            SqlServerOrdersEventInfo sqlServerOrdersEventInfo = sqlServerOrdersEventData.getBefore();
            sqlServerOrdersEventInfo.set_row_kind(sqlServerOrdersEventData.getOp());

            sqlServerOrdersEventInfo.set_source_time(sqlServerOrdersEventData.getSourceTime());
            sqlServerOrdersEventInfo.set_ingestion_time(sqlServerOrdersEventData.getIngestionTime());
            sqlServerOrdersEventInfo.set_process_time(new Date());

            collector.collect(sqlServerOrdersEventData.getBefore());
        } else {
            // 可以选择记录日志、跳过或者处理异常数据
            log.warn("Received null data or info is null: " + sqlServerOrdersEventData);
        }

    }

}
