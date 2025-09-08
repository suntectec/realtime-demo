package com.sands.realtime.common.bean.ods;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Jagger
 * @since 2025/8/26 14:43
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SqlServerOrdersInfo {

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
    private String _proc_time;

}
