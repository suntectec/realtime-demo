package com.sands.realtime.ods.bean;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Date;


/**
 * @author Jagger
 * @since 2025/8/22 15:13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SqlServerOrdersBean {

    @JSONField(name = "ingestionTime", format = "yyyy-MM-dd HH:mm:ss")
    private Date ingestionTime;

    @JSONField(name = "op")
    private String op;

    @JSONField(name = "sourceTime", format = "yyyy-MM-dd HH:mm:ss")
    private Date sourceTime;

    @JSONField(name = "tableCatalog")
    private String tableCatalog;

    @JSONField(name = "tableSchema")
    private String tableSchema;

    @JSONField(name = "tableName")
    private String tableName;

    @JSONField(name = "after")
    private SqlServerOrdersInfo after;

}
