package com.cpeplatform.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 延迟数据流的模型，将被发送到 cpe-processed-latency 主题。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CpeLatencyData {
    private String deviceId;
    private int rtt;
    private long timestamp;
}
