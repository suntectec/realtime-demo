package com.sands.realtime.common.bean.ods;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author Jagger
 * @since 2025/8/26 14:43
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SqlServerOrdersInfo {

    @JSONField(name = "id")
    private String id;

    @JSONField(name = "order_id")
    private String orderId;

    @JSONField(name = "supplier_id")
    private String supplierId;

    @JSONField(name = "item_id")
    private String itemId;

    @JSONField(name = "status")
    private String status;

    @JSONField(name = "qty")
    private String qty;

    @JSONField(name = "net_price")
    private String netPrice;

    @JSONField(name = "issued_at")
    private String issuedAt;

    @JSONField(name = "completed_at")
    private String completedAt;

    @JSONField(name = "spec")
    private String spec;

    @JSONField(name = "created_at")
    private String createdAt;

    @JSONField(name = "updated_at")
    private String updatedAt;

    @JSONField(name = "_row_kind")
    private String _rowKind;

    @JSONField(name = "_proc_time")
    private String _procTime;
}
