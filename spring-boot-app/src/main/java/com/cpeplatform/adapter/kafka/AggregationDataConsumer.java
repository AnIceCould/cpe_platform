package com.cpeplatform.adapter.kafka;

import com.cpeplatform.dto.CpePacketLossAggregation;
import com.cpeplatform.dto.LatencyDataPoint;
import com.cpeplatform.service.PredictionClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

/**
 * 负责消费 Flink 聚合数据，并触发 gRPC 预测服务。
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

    @KafkaListener(topics = "${app.kafka.topic.packetloss-aggregation}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeAggregationData(CpePacketLossAggregation aggregation) {
        logger.info("=================================================");
        logger.info("  📬 接收到 Flink 聚合数据，准备进行预测...");
        logger.info("-------------------------------------------------");
        logger.info("  ▶ 设备ID: {}", aggregation.getDeviceId());

        // 1. 从接收到的数据中提取出 RTT 列表
        var rtts = aggregation.getLatencyDataPoints().stream()
                .map(LatencyDataPoint::getRtt)
                .collect(Collectors.toList());

        // 2. 调用 gRPC 服务进行预测
        predictionClientService.predict(rtts);

        logger.info("=================================================");
    }
}

