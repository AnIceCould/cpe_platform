package com.cpeplatform.service;

import com.cpeplatform.adapter.kafka.PredictionResultProducerService;
import com.cpeplatform.config.ExecutorConfig;
import com.cpeplatform.dto.CpeFeatures;
import com.cpeplatform.dto.PredictionResultDto;
import com.cpeplatform.grpc.PacketLossFeaturesRequest;
import com.cpeplatform.grpc.PacketLossResponse;
import com.cpeplatform.grpc.PredictionServiceGrpc;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 负责与外部的 gRPC 预测服务进行通信。
 * 采用手动轮询方式。
 */
@Service
public class PredictionClientService {

    private static final Logger logger = LoggerFactory.getLogger(PredictionClientService.class);

    private static final String GRPC_SERVER_1_ADDRESS = "localhost";
    private static final int GRPC_SERVER_1_PORT = 9090;
    private static final String GRPC_SERVER_2_ADDRESS = "localhost";
    private static final int GRPC_SERVER_2_PORT = 9091;

    // 为每个服务器创建一个独立的 Channel 和 Stub
    private ManagedChannel channel1;
    private ManagedChannel channel2;
    private PredictionServiceGrpc.PredictionServiceFutureStub futureStub1;
    private PredictionServiceGrpc.PredictionServiceFutureStub futureStub2;

    // 一个用于轮询的线程安全的计数器
    private final AtomicInteger requestCounter = new AtomicInteger(0);

    private final Executor grpcCallbackExecutor;
    private final PredictionResultProducerService producerService;

    @Autowired
    public PredictionClientService(
            PredictionResultProducerService producerService,
            @Qualifier(ExecutorConfig.GRPC_CALLBACK_EXECUTOR) Executor grpcCallbackExecutor) {
        this.producerService = producerService;
        this.grpcCallbackExecutor = grpcCallbackExecutor;
    }

    @PostConstruct
    private void init() {
        logger.info("正在初始化 gRPC 客户端 (手动轮询模式)...");

        // 创建到服务器1的连接
        logger.info(" -> 连接到服务器1: {}:{}", GRPC_SERVER_1_ADDRESS, GRPC_SERVER_1_PORT);
        channel1 = ManagedChannelBuilder.forAddress(GRPC_SERVER_1_ADDRESS, GRPC_SERVER_1_PORT)
                .usePlaintext()
                .build();
        futureStub1 = PredictionServiceGrpc.newFutureStub(channel1);

        // 创建到服务器2的连接
        logger.info(" -> 连接到服务器2: {}:{}", GRPC_SERVER_2_ADDRESS, GRPC_SERVER_2_PORT);
        channel2 = ManagedChannelBuilder.forAddress(GRPC_SERVER_2_ADDRESS, GRPC_SERVER_2_PORT)
                .usePlaintext()
                .build();
        futureStub2 = PredictionServiceGrpc.newFutureStub(channel2);

        logger.info("gRPC 客户端已准备就绪。");
    }

    public void predict(CpeFeatures features) {
        if (features == null) {
            logger.warn("输入的特征对象为空，无法进行预测。");
            return;
        }

        try {
            // 手动实现轮询负载均衡
            int currentRequest = requestCounter.getAndIncrement();
            PredictionServiceGrpc.PredictionServiceFutureStub selectedStub;
            String selectedServer;

            if (currentRequest % 2 == 0) {
                selectedStub = futureStub1;
                selectedServer = GRPC_SERVER_1_ADDRESS + ":" + GRPC_SERVER_1_PORT;
            } else {
                selectedStub = futureStub2;
                selectedServer = GRPC_SERVER_2_ADDRESS + ":" + GRPC_SERVER_2_PORT;
            }

            logger.info("准备【异步】调用 gRPC 预测服务 (目标: {})", selectedServer);

            PacketLossFeaturesRequest request = PacketLossFeaturesRequest.newBuilder()
                    .setDelay1(features.getDelay_1()).setDelay2(features.getDelay_2()).setDelay3(features.getDelay_3())
                    .setDelay4(features.getDelay_4()).setDelay5(features.getDelay_5())
                    .setMeanDelay(features.getMean_delay()).setMinDelay(features.getMin_delay())
                    .setMidDelay(features.getMid_delay()).setMaxDelay(features.getMax_delay())
                    .setRange(features.getRange()).setMeanOfLastThree(features.getMean_of_last_three())
                    .setDiffBetweenLastTwo(features.getDiff_between_last_two()).setSlopeDelay(features.getSlope_delay())
                    .build();

            ListenableFuture<PacketLossResponse> futureResponse = selectedStub.predictPacketLoss(request);

            Futures.addCallback(futureResponse, new FutureCallback<>() {
                @Override
                public void onSuccess(PacketLossResponse response) {
                    boolean prediction = response.getHasPacketLoss();
                    logger.info("✅ 异步 gRPC 调用成功 (来自 {})！预测结果: {}", selectedServer, prediction ? "可能丢包" : "正常");

                    PredictionResultDto resultDto = PredictionResultDto.builder()
                            .deviceId(features.getDeviceId())
                            .predictionTimestamp(System.currentTimeMillis())
                            .hasPacketLoss(prediction)
                            .build();

                    producerService.sendPredictionResult(resultDto);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("❌ 异步 gRPC 服务调用失败 (目标: {}): {}", selectedServer, t.getMessage());
                }
            }, grpcCallbackExecutor);

        } catch (Exception e) {
            logger.error("❌ 构建 gRPC 请求时发生同步错误: {}", e.getMessage());
        }
    }

    @PreDestroy
    private void shutdown() throws InterruptedException {
        // 【核心修正】: 关闭所有 channel
        logger.info("正在关闭 gRPC 连接...");
        if (channel1 != null) {
            channel1.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (channel2 != null) {
            channel2.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        logger.info("gRPC 连接已关闭。");
    }
}

