package com.cpeplatform.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

/**
 * 代表5次丢包数据聚合结果的POJO。
 * 这是最终发送到 'cpe-packetloss-aggregation' Topic 的消息格式。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CpePacketLossAggregation {
    /**
     * 设备ID
     */
    private String deviceId;

    /**
     * 聚合完成时的时间戳
     */
    private long aggregationTimestamp;

    /**
     * 包含5个延迟数据点的列表
     */
    private List<LatencyDataPoint> latencyDataPoints;
}
