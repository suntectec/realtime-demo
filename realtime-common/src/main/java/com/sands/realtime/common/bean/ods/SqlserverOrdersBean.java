package com.sands.realtime.common.bean.ods;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author Jagger
 * @since 2025/8/22 15:13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SqlserverOrdersBean {

    @JSONField(name = "ingestionDateTime")
    private String ingestionDateTime;

    @JSONField(name = "op")
    private String op;

    @JSONField(name = "sourceDateTime")
    private String sourceDateTime;

    @JSONField(name = "tableCatalog")
    private String tableCatalog;

    @JSONField(name = "tableSchema")
    private String tableSchema;

    @JSONField(name = "tableName")
    private String tableName;

    @JSONField(alternateNames = {"after"}, name = "data")
    private SqlserverOrdersInfo after;

}
