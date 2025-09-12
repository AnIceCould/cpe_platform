package com.cpeplatform.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 状态数据流的模型，将被发送到 cpe-processed-status 主题。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CpeStatusData {
    private String deviceId;
    private String status;
    private long timestamp;
}
