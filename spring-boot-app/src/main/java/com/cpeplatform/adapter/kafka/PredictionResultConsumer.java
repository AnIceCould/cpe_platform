package com.cpeplatform.adapter.kafka;

import com.cpeplatform.dto.PredictionResultDto;
import com.cpeplatform.service.AlertService;
import com.cpeplatform.service.CpeCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 负责消费预测结果，并将其委托给相关服务进行处理。
 */
@Service
public class PredictionResultConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PredictionResultConsumer.class);

    private final AlertService alertService;
    private final CpeCacheService cpeCacheService;

    // 更新构造函数以接收新的服务
    public PredictionResultConsumer(AlertService alertService, CpeCacheService cpeCacheService) {
        this.alertService = alertService;
        this.cpeCacheService = cpeCacheService;
    }

    @KafkaListener(topics = "${app.kafka.topic.prediction-result}", groupId = "${spring.kafka.consumer.group-id}-alerter", containerFactory = "predictionResultKafkaListenerContainerFactory")
    public void consumePredictionResult(PredictionResultDto resultDto) {

        if (resultDto.isHasPacketLoss()) {
            logger.info("📬 检测到丢包事件，准备处理... 设备ID: {}", resultDto.getDeviceId());

            // 立即更新Redis中该设备的最新丢包状态缓存。
            cpeCacheService.updateLatestPacketLossEvent(resultDto);

            // 将事件委托给警报服务进行后续处理 (推送WebSocket和异步入库)
            alertService.processPacketLossAlert(resultDto);

        } else {
            logger.debug("接收到正常事件，无需处理。设备ID: {}", resultDto.getDeviceId());
        }
    }
}