package com.cpeplatform.adapter.kafka;

import com.cpeplatform.adapter.websocket.AlertWebsocketHandler;
import com.cpeplatform.dto.PredictionResultDto;
import com.cpeplatform.persistence.entity.PacketLossEvent;
import com.cpeplatform.persistence.repository.PacketLossEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PacketLossPersistenceConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PacketLossPersistenceConsumer.class);

    private final PacketLossEventRepository repository;
    private final AlertWebsocketHandler websocketHandler; // 【新增】注入WebSocket处理器

    // 【修改】更新构造函数
    public PacketLossPersistenceConsumer(PacketLossEventRepository repository, AlertWebsocketHandler websocketHandler) {
        this.repository = repository;
        this.websocketHandler = websocketHandler;
    }

    @KafkaListener(topics = "${app.kafka.topic.prediction-result}", groupId = "${spring.kafka.consumer.group-id}-db-persister", containerFactory = "predictionResultKafkaListenerContainerFactory")
    @Transactional
    public void consumeAndPersistPacketLoss(PredictionResultDto resultDto) {

        if (resultDto.isHasPacketLoss()) {

            logger.info("📬 检测到丢包事件，准备处理... 设备ID: {}", resultDto.getDeviceId());

            PacketLossEvent event = PacketLossEvent.builder()
                    .deviceId(resultDto.getDeviceId())
                    .eventTimestamp(resultDto.getPredictionTimestamp())
                    .hasPacketLoss(true)
                    .build();

            try {
                // 1. 先将事件保存到数据库
                repository.save(event);
                logger.info("✅ 丢包事件已成功保存到MySQL数据库！");

                // 2. 【【【核心新增逻辑】】】
                //    在数据成功入库后，立即通过 WebSocket 推送警报
                websocketHandler.sendPacketLossAlert(resultDto);

            } catch (Exception e) {
                logger.error("❌ 处理丢包事件时发生错误", e);
                throw new RuntimeException("处理丢包事件失败", e);
            }
        } else {
            logger.debug("接收到正常事件，无需处理。设备ID: {}", resultDto.getDeviceId());
        }
    }
}

