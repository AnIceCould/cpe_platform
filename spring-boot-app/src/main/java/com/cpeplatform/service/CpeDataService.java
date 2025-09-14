package com.cpeplatform.service;

import com.cpeplatform.api.dto.CpeRawDataDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * 负责处理CPE相关数据的业务逻辑，例如将数据发送到Kafka。
 */
@Service
public class CpeDataService {

    private static final Logger logger = LoggerFactory.getLogger(CpeDataService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // 从 application.yml 配置文件中注入要发送的Topic名称
    @Value("${app.kafka.topic.raw-data}")
    private String rawDataTopic;

    /**
     * 构造函数注入依赖。
     * @param kafkaTemplate Spring Kafka 提供的生产者客户端
     * @param objectMapper Jackson JSON 处理工具
     */
    public CpeDataService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 将接收到的CPE原始数据发送到Kafka。
     * @param dataDto 从API接收到的数据传输对象
     */
    public void sendRawDataToKafka(CpeRawDataDto dataDto) {
        try {
            // 1. 将 DTO 对象序列化为 JSON 字符串
            String messagePayload = objectMapper.writeValueAsString(dataDto);

            // 2. 使用 KafkaTemplate 发送消息
            kafkaTemplate.send(rawDataTopic, dataDto.getDeviceId(), messagePayload);

            logger.info("✅ 已通过API成功接收并发送数据到Kafka: {}", messagePayload);

        } catch (JsonProcessingException e) {
            logger.error("❌ 将API接收的DTO序列化为JSON时出错", e);
            // 在实际应用中，这里可以抛出一个自定义异常，让Controller层捕获并返回500错误
        }
    }
}
