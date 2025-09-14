package com.cpeplatform.flink.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 代表从5个RTT值中提取出的完整特征集。
 * 这是 Flink 作业的最终产出，也是 gRPC 服务的输入。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CpeFeatures {
    private String deviceId;
    private long aggregationTimestamp;

    // 原始 RTT 值
    private int delay_1;
    private int delay_2;
    private int delay_3;
    private int delay_4;
    private int delay_5;

    // 统计特征
    private double mean_delay;
    private double min_delay;
    private double mid_delay; // Median
    private double max_delay;
    private double range;
    private double mean_of_last_three;
    private double diff_between_last_two;

    // 趋势特征
    private double slope_delay;
}
