package com.sands.realtime.common.bean.ods;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author Jagger
 * @since 2025/8/22 15:13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SqlServerOrdersEventData {

    @JSONField(name = "ingestion_time", format = "yyyy-MM-dd HH:mm:ss")
    private Date ingestionTime;

    private String op;

    @JSONField(name = "source_time", format = "yyyy-MM-dd HH:mm:ss")
    private Date sourceTime;

    @JSONField(name = "table_catalog")
    private String tableCatalog;

    @JSONField(name = "table_schema")
    private String tableSchema;

    @JSONField(name = "table_name")
    private String tableName;

    private SqlServerOrdersEventInfo after;

    private SqlServerOrdersEventInfo before;
}
