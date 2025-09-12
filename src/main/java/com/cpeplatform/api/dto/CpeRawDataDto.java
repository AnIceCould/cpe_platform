package com.cpeplatform.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * CPE原始数据的数据传输对象 (DTO).
 * 用于定义发送到Kafka的原始消息的结构。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CpeRawDataDto {

    /**
     * 设备唯一ID
     */
    private String deviceId;

    /**
     * 设备状态 (例如: "ONLINE", "OFFLINE", "DEGRADED")
     */
    private String status;

    /**
     * 往返时延 (Round-Trip Time), 单位: 毫秒(ms)
     */
    private int rtt;

    /**
     * 事件发生的时间戳 (Unix-Timestamp, 毫秒)
     */
    private long timestamp;
}
