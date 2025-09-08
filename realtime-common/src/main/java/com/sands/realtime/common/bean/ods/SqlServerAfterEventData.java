package com.sands.realtime.common.bean.ods;

import lombok.*;

/**
 * @author Jagger
 * @since 2025/8/26 14:43
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SqlServerAfterEventData {

    private String id;
    private String orderId;
    private String supplierId;
    private String itemId;
    private String status;
    private String qty;
    private String netPrice;
    private String issuedAt;
    private String completedAt;
    private String spec;
    private String createdAt;
    private String updatedAt;
    private String _rowKind;
    private String _procTime;

}
