package com.cpeplatform.adapter.kafka;

import com.cpeplatform.dto.CpeFeatures; // 【重要】导入新的共享 DTO
import com.cpeplatform.service.PredictionClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 负责消费 Flink 计算好的特征数据，并触发 gRPC 预测服务。
 */
@Service
public class AggregationDataConsumer {

    private static final Logger logger = LoggerFactory.getLogger(AggregationDataConsumer.class);

    // 注入我们新建的 gRPC 客户端服务
    private final PredictionClientService predictionClientService;

    @Autowired
    public AggregationDataConsumer(PredictionClientService predictionClientService) {
        this.predictionClientService = predictionClientService;
    }

    /**
     * 【核心修正】
     * 1. 监听的 Topic 更新为 Flink 输出特征数据的新 Topic。
     * 2. 接收的参数类型直接就是 Flink 计算好的 CpeFeatures 对象。
     */
    @KafkaListener(topics = "${app.kafka.topic.features-for-prediction}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "featuresKafkaListenerContainerFactory")
    public void consumeFeaturesData(CpeFeatures features) {
        logger.info("=================================================");
        logger.info("  📬 接收到 Flink 计算的特征集，准备进行预测...");
        logger.info("-------------------------------------------------");
        logger.info("  ▶ 设备ID: {}", features.getDeviceId());

        // 【核心修正】
        // 不再需要提取RTT列表，直接将整个特征对象传递给预测服务。
        predictionClientService.predict(features);

        logger.info("=================================================");
    }
}

