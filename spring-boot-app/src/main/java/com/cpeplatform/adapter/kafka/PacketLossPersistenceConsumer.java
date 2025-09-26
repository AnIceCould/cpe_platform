package com.cpeplatform.adapter.kafka;

import com.cpeplatform.dto.PredictionResultDto;
import com.cpeplatform.service.PacketLossPersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 职责：消费预测结果，并将其委托给持久化服务写入数据库。
 */
@Service
public class PacketLossPersistenceConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PacketLossPersistenceConsumer.class);

    private final PacketLossPersistenceService persistenceService;

    public PacketLossPersistenceConsumer(PacketLossPersistenceService persistenceService) {
        this.persistenceService = persistenceService;
    }

    @KafkaListener(
            topics = "${app.kafka.topic.prediction-result}",
            // 【核心】: 使用一个专门用于持久化的、独立的消费者组ID
            groupId = "${spring.kafka.consumer.group-id}-persister",
            containerFactory = "predictionResultKafkaListenerContainerFactory"
    )
    public void consumeAndPersist(PredictionResultDto resultDto) {

        if (resultDto.isHasPacketLoss()) {

            logger.info("📬 [持久化消费者] 检测到丢包事件，交由持久化服务处理... 设备ID: {}", resultDto.getDeviceId());

            persistenceService.persistEvent(resultDto);

        } else {
            logger.debug("[持久化消费者] 接收到正常事件，无需持久化。设备ID: {}", resultDto.getDeviceId());
        }
    }
}

