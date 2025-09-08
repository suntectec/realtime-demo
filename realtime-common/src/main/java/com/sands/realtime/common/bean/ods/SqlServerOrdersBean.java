package com.sands.realtime.common.bean.ods;

import com.alibaba.fastjson2.annotation.JSONField;
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
public class SqlServerOrdersBean {

    @JSONField(name = "ingestionTime", format = "yyyy-MM-dd HH:mm:ss")
    private Date ingestionTime;

    private String op;

    @JSONField(name = "sourceTime", format = "yyyy-MM-dd HH:mm:ss")
    private Date sourceTime;

    private String tableCatalog;

    private String tableSchema;

    private String tableName;

    private SqlServerOrdersInfo after;

}
