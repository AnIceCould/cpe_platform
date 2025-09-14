package com.cpeplatform.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 代表单个延迟数据点的POJO。
 * 用于在最终的聚合结果中作为列表的一个元素。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LatencyDataPoint {
    /**
     * 延迟时间 (Round-Trip Time)
     */
    private int rtt;

    /**
     * 原始事件的时间戳
     */
    private long timestamp;
}
