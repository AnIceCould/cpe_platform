package com.cpeplatform.config;

import com.cpeplatform.dto.CpeFeatures;
import com.cpeplatform.dto.CpeStatusDataDto;
import com.cpeplatform.dto.PredictionResultDto;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

/**
 * 负责定义所有 Kafka 消费者的专属配置工厂 (ContainerFactory)。
 * Spring Boot 会自动扫描并加载这个带有 @Configuration 注解的类。
 */
@Configuration
public class KafkaConsumerConfig {

    private final KafkaProperties properties;

    public KafkaConsumerConfig(KafkaProperties properties) {
        this.properties = properties;
    }

    // --- CpeFeatures 消费者的配置 ---
    @Bean
    public ConsumerFactory<String, CpeFeatures> featuresConsumerFactory() {
        Map<String, Object> props = properties.buildConsumerProperties();
        JsonDeserializer<CpeFeatures> deserializer = new JsonDeserializer<>(CpeFeatures.class);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CpeFeatures> featuresKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, CpeFeatures> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(featuresConsumerFactory());
        return factory;
    }

    // --- PredictionResultDto 消费者的配置 ---
    @Bean
    public ConsumerFactory<String, PredictionResultDto> predictionResultConsumerFactory() {
        Map<String, Object> props = properties.buildConsumerProperties();
        JsonDeserializer<PredictionResultDto> deserializer = new JsonDeserializer<>(PredictionResultDto.class);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PredictionResultDto> predictionResultKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, PredictionResultDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(predictionResultConsumerFactory());
        return factory;
    }

    // 为 CpeStatusDataDto 创建专属的消费者工厂和 ListenerContainerFactory
    @Bean
    public ConsumerFactory<String, CpeStatusDataDto> statusConsumerFactory() {
        Map<String, Object> props = properties.buildConsumerProperties();
        // 配置JsonDeserializer以处理本模块的 CpeStatusDataDto 类
        JsonDeserializer<CpeStatusDataDto> deserializer = new JsonDeserializer<>(CpeStatusDataDto.class);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CpeStatusDataDto> statusKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, CpeStatusDataDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(statusConsumerFactory());
        return factory;
    }
}

