package com.cpeplatform.service;

import com.cpeplatform.adapter.kafka.PredictionResultProducerService;
import com.cpeplatform.dto.CpeFeatures;
import com.cpeplatform.dto.PredictionResultDto;
import com.cpeplatform.grpc.PacketLossFeaturesRequest;
import com.cpeplatform.grpc.PacketLossResponse;
import com.cpeplatform.grpc.PredictionServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

/**
 * 负责与外部的 gRPC 预测服务进行通信。
 */
@Service
public class PredictionClientService {

    private static final Logger logger = LoggerFactory.getLogger(PredictionClientService.class);

    @Value("${grpc.server.address}")
    private String grpcServerAddress;

    @Value("${grpc.server.port}")
    private int grpcServerPort;

    private ManagedChannel channel;
    private PredictionServiceGrpc.PredictionServiceBlockingStub blockingStub;

    // 注入新的生产者服务
    private final PredictionResultProducerService producerService;
    @Autowired
    public PredictionClientService(PredictionResultProducerService producerService) {
        this.producerService = producerService;
    }

    @PostConstruct
    private void init() {
        logger.info("正在连接到 gRPC 服务器，地址: {}:{}", grpcServerAddress, grpcServerPort);
        channel = ManagedChannelBuilder.forAddress(grpcServerAddress, grpcServerPort)
                .usePlaintext()
                .build();
        blockingStub = PredictionServiceGrpc.newBlockingStub(channel);
    }

    /**
     * 调用 gRPC 服务进行丢包预测。
     * @param features 从 Flink 接收到的完整特征集
     * @return 预测结果，true表示可能丢包，false表示正常
     */
    public boolean predict(CpeFeatures features) {
        if (features == null) {
            logger.warn("输入的特征对象为空，无法进行预测。");
            return false;
        }

        try {
            logger.info("准备调用 gRPC 预测服务...");
            // 1. 构建 gRPC 请求对象
            PacketLossFeaturesRequest request = PacketLossFeaturesRequest.newBuilder()
                    .setDelay1(features.getDelay_1())
                    .setDelay2(features.getDelay_2())
                    .setDelay3(features.getDelay_3())
                    .setDelay4(features.getDelay_4())
                    .setDelay5(features.getDelay_5())
                    .setMeanDelay(features.getMean_delay())
                    .setMinDelay(features.getMin_delay())
                    .setMidDelay(features.getMid_delay())
                    .setMaxDelay(features.getMax_delay())
                    .setRange(features.getRange())
                    .setMeanOfLastThree(features.getMean_of_last_three())
                    .setDiffBetweenLastTwo(features.getDiff_between_last_two())
                    .setSlopeDelay(features.getSlope_delay())
                    .build();

            // 2. 发送请求并获取响应
            PacketLossResponse response = blockingStub.predictPacketLoss(request);
            boolean prediction = response.getHasPacketLoss();
            logger.info("✅ gRPC 服务调用成功！预测结果: {}", prediction ? "可能丢包" : "正常");
            // 3. 【【【核心新增逻辑】】】
            //    构建最终的预测结果对象
            PredictionResultDto resultDto = PredictionResultDto.builder()
                    .deviceId(features.getDeviceId())
                    .predictionTimestamp(System.currentTimeMillis())
                    .hasPacketLoss(prediction)
                    .build();

            // 4. 调用生产者服务，将结果发送到 Kafka
            producerService.sendPredictionResult(resultDto);
            return true;
        } catch (Exception e) {
            logger.error("❌ gRPC 服务调用失败: {}", e.getMessage());
            return false;
        }
    }

    @PreDestroy
    private void shutdown() throws InterruptedException {
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}

