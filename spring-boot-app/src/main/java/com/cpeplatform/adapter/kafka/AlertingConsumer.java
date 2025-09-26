package com.cpeplatform.adapter.kafka;

import com.cpeplatform.adapter.websocket.AlertWebsocketHandler;
import com.cpeplatform.dto.PredictionResultDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 一个专门的消费者，其唯一职责是实时推送WebSocket警报。
 */
@Service
public class AlertingConsumer {

    private static final Logger logger = LoggerFactory.getLogger(AlertingConsumer.class);

    private final AlertWebsocketHandler websocketHandler;

    public AlertingConsumer(AlertWebsocketHandler websocketHandler) {
        this.websocketHandler = websocketHandler;
    }

    /**
     * 监听 'cpe-prediction-result' 主题。
     * @param resultDto 从Kafka接收到的预测结果对象
     */
    @KafkaListener(
            topics = "${app.kafka.topic.prediction-result}",
            // 【核心】: 使用一个专门用于警报的、独立的消费者组ID
            groupId = "${spring.kafka.consumer.group-id}-alerter",
            containerFactory = "predictionResultKafkaListenerContainerFactory"
    )
    public void consumeAndPushAlert(PredictionResultDto resultDto) {
        if (resultDto.isHasPacketLoss()) {
            logger.info("📬 [警报消费者] 检测到丢包事件，准备通过WebSocket推送... 设备ID: {}", resultDto.getDeviceId());
            try {
                // 直接调用WebSocket处理器进行广播
                websocketHandler.sendPacketLossAlert(resultDto);
            } catch (Exception e) {
                logger.error("❌ [警报消费者] 推送WebSocket警报时发生错误", e);
            }
        } else {
            logger.debug("[警报消费者] 接收到正常事件，无需推送。设备ID: {}", resultDto.getDeviceId());
        }
    }
}
