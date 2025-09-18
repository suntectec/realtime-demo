package com.sands.realtime.common.bean.ods;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author Jagger
 * @since 2025/8/26 14:43
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SqlServerOrdersEventInfo {

    private String id;
    private String order_id;
    private String supplier_id;
    private String item_id;
    private String status;
    private String qty;
    private String net_price;
    private String issued_at;
    private String completed_at;
    private String spec;
    private String created_at;
    private String updated_at;

    private String _row_kind;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date _source_time;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date _ingestion_time;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private Date _process_time;

}
