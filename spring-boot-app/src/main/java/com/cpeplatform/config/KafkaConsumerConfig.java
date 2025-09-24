package com.cpeplatform.config;

import com.cpeplatform.dto.CpeFeatures;
import com.cpeplatform.dto.PredictionResultDto;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Autowired
    private KafkaProperties properties; // 从Spring Boot自动配置中获取基础属性

    /**
     * 为 CpeFeatures 创建一个专属的、完全在代码中配置的消费者工厂。
     */
    @Bean
    public ConsumerFactory<String, CpeFeatures> featuresConsumerFactory() {
        // 1. 获取 application.yml 中的基础配置 (如 bootstrap-servers, group-id)
        Map<String, Object> props = properties.buildConsumerProperties();

        // 2. 创建一个专门用于 CpeFeatures 的反序列化器
        JsonDeserializer<CpeFeatures> deserializer = new JsonDeserializer<>(CpeFeatures.class);

        // 3. 创建消费者工厂，并传入我们手动创建的反序列化器实例
        // 这样可以完全避免 Spring 尝试从 props 中读取冲突的配置
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    /**
     * 为 CpeFeatures 消费者创建专属的 ListenerContainerFactory。
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CpeFeatures> featuresKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, CpeFeatures> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(featuresConsumerFactory());
        return factory;
    }

    /**
     * 为 PredictionResultDto 创建一个专属的、完全在代码中配置的消费者工厂。
     */
    @Bean
    public ConsumerFactory<String, PredictionResultDto> predictionResultConsumerFactory() {
        Map<String, Object> props = properties.buildConsumerProperties();

        JsonDeserializer<PredictionResultDto> deserializer = new JsonDeserializer<>(PredictionResultDto.class);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    /**
     * 为 PredictionResultDto 消费者创建专属的 ListenerContainerFactory。
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PredictionResultDto> predictionResultKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, PredictionResultDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(predictionResultConsumerFactory());
        return factory;
    }
}

