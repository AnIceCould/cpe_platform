package com.cpeplatform.adapter.kafka;

import com.cpeplatform.dto.PredictionResultDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * 负责将预测结果发送到 Kafka 的专用服务。
 */
@Service
public class PredictionResultProducerService {

    private static final Logger logger = LoggerFactory.getLogger(PredictionResultProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // 从 application.yml 注入新的 Topic 名称
    @Value("${app.kafka.topic.prediction-result}")
    private String predictionResultTopic;

    public PredictionResultProducerService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 将 PredictionResultDto 对象序列化为 JSON 并发送到 Kafka。
     * @param resultDto 包含预测结果的DTO
     */
    public void sendPredictionResult(PredictionResultDto resultDto) {
        try {
            String messagePayload = objectMapper.writeValueAsString(resultDto);
            // 使用 deviceId 作为 Key，确保相同设备的结果进入同一分区
            kafkaTemplate.send(predictionResultTopic, resultDto.getDeviceId(), messagePayload);
            logger.info("✅ 已成功将预测结果发送到 Kafka Topic [{}]: {}", predictionResultTopic, messagePayload);
        } catch (JsonProcessingException e) {
            logger.error("❌ 序列化预测结果DTO时出错", e);
        }
    }
}
