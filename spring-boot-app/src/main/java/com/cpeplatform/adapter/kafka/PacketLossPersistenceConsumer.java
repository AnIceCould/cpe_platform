package com.cpeplatform.adapter.kafka;

import com.cpeplatform.adapter.websocket.AlertWebsocketHandler;
import com.cpeplatform.dto.PredictionResultDto;
import com.cpeplatform.persistence.entity.PacketLossEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 负责消费预测结果，并在检测到丢包时，将事件快速写入Redis缓冲区。
 */
@Service
public class PacketLossPersistenceConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PacketLossPersistenceConsumer.class);

    // Redis中用于缓冲事件的列表的Key
    public static final String PACKET_LOSS_BUFFER_KEY = "packetloss:events:buffer";

    private final AlertWebsocketHandler websocketHandler;
    private final RedisTemplate<String, Object> redisTemplate;

    public PacketLossPersistenceConsumer(AlertWebsocketHandler websocketHandler, RedisTemplate<String, Object> redisTemplate) {
        this.websocketHandler = websocketHandler;
        this.redisTemplate = redisTemplate;
    }

    // KafkaListener配置保持不变
    @KafkaListener(topics = "${app.kafka.topic.prediction-result}", groupId = "${spring.kafka.consumer.group-id}-persister", containerFactory = "predictionResultKafkaListenerContainerFactory")
    public void consumeAndBufferPacketLoss(PredictionResultDto resultDto) {

        if (resultDto.isHasPacketLoss()) {

            logger.info("📬 检测到丢包事件，准备写入Redis缓冲... 设备ID: {}", resultDto.getDeviceId());

            PacketLossEvent event = PacketLossEvent.builder()
                    .deviceId(resultDto.getDeviceId())
                    .eventTimestamp(resultDto.getPredictionTimestamp())
                    .hasPacketLoss(true)
                    .build();

            try {
                // 1. 将事件对象推入Redis列表的末尾
                redisTemplate.opsForList().rightPush(PACKET_LOSS_BUFFER_KEY, event);
                logger.debug(" -> 事件已成功写入Redis缓冲。");

                // 2. 立即通过 WebSocket 推送警报
                websocketHandler.sendPacketLossAlert(resultDto);

            } catch (Exception e) {
                logger.error("❌ 写入Redis缓冲时发生错误", e);
                // 在生产环境中，这里可以加入重试或告警逻辑
            }
        } else {
            logger.debug("接收到正常事件，无需处理。设备ID: {}", resultDto.getDeviceId());
        }
    }
}

