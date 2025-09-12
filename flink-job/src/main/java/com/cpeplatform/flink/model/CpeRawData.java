package com.cpeplatform.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Flink作业的输入数据模型，对应于 cpe-raw-data 主题中的JSON结构。
 * 使用Lombok简化代码。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CpeRawData {
    private String deviceId;
    private String status;
    private int rtt;
    private long timestamp;
}
