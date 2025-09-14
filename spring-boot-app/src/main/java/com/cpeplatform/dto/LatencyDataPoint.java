package com.cpeplatform.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 代表单个延迟数据点的POJO。
 * 在 Flink 和 Spring Boot 应用之间共享。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LatencyDataPoint {
    private int rtt;
    private long timestamp;
}
