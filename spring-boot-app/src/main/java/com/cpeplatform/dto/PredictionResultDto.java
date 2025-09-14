package com.cpeplatform.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 代表最终预测结果的数据模型。
 * 这个对象将被发送到 'cpe-prediction-result' Topic。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PredictionResultDto {
    /**
     * 设备ID
     */
    private String deviceId;

    /**
     * 预测发生时的时间戳
     */
    private long predictionTimestamp;

    /**
     * 预测结果 (true 表示可能丢包, false 表示正常)
     */
    private boolean hasPacketLoss;
}
