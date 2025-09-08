package com.sands.realtime.common.bean.ods;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.Map;

/**
 * @author Jagger
 * @since 2025/8/22 15:13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SqlServerEventData {

    @JsonProperty("before")
    private Map<String, Object> before;

    @JsonProperty("after")
    private SqlServerAfterEventData after;

    @JsonProperty("source")
    private Map<String, Object> source;

    @JsonProperty("op")
    private String op;

    @JsonProperty("ts_ms")
    private Long tsMs;

    @JsonProperty("transaction")
    private Object transaction;

}
