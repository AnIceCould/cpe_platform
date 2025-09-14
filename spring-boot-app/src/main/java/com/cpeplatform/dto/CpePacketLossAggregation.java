package com.cpeplatform.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

/**
 * 代表5次丢包数据聚合结果的POJO。
 * 在 Flink 和 Spring Boot 应用之间共享。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CpePacketLossAggregation {
    private String deviceId;
    private long aggregationTimestamp;
    private List<LatencyDataPoint> latencyDataPoints;
}
