package com.cpeplatform.service;

import com.cpeplatform.adapter.kafka.PredictionResultProducerService;
import com.cpeplatform.config.ExecutorConfig; // 导入我们的配置类
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
import org.springframework.beans.factory.annotation.Qualifier; // 【新增导入】
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.Executor; // 【修改导入】
import java.util.concurrent.TimeUnit;

@Service
public class PredictionClientService {

    private static final Logger logger = LoggerFactory.getLogger(PredictionClientService.class);

    @Value("${grpc.server.address}")
    private String grpcServerAddress;
    @Value("${grpc.server.port}")
    private int grpcServerPort;

    private ManagedChannel channel;
    private PredictionServiceGrpc.PredictionServiceFutureStub futureStub;

    // 【核心修改】: 注入我们自定义的 Executor
    private final Executor grpcCallbackExecutor;
    private final PredictionResultProducerService producerService;

    @Autowired
    public PredictionClientService(
            PredictionResultProducerService producerService,
            // 使用 @Qualifier 注解，确保 Spring 注入的是我们刚刚定义的那个 Bean
            @Qualifier(ExecutorConfig.GRPC_CALLBACK_EXECUTOR) Executor grpcCallbackExecutor) {
        this.producerService = producerService;
        this.grpcCallbackExecutor = grpcCallbackExecutor;
    }

    @PostConstruct
    private void init() {
        logger.info("正在连接到 gRPC 服务器，地址: {}:{}", grpcServerAddress, grpcServerPort);
        channel = ManagedChannelBuilder.forAddress(grpcServerAddress, grpcServerPort)
                .usePlaintext()
                .build();
        futureStub = PredictionServiceGrpc.newFutureStub(channel);
    }

    public void predict(CpeFeatures features) {
        if (features == null) {
            logger.warn("输入的特征对象为空，无法进行预测。");
            return;
        }

        try {
            logger.info("准备【异步】调用 gRPC 预测服务...");
            PacketLossFeaturesRequest request = PacketLossFeaturesRequest.newBuilder()
                    .setDelay1(features.getDelay_1()).setDelay2(features.getDelay_2()).setDelay3(features.getDelay_3())
                    .setDelay4(features.getDelay_4()).setDelay5(features.getDelay_5())
                    .setMeanDelay(features.getMean_delay()).setMinDelay(features.getMin_delay())
                    .setMidDelay(features.getMid_delay()).setMaxDelay(features.getMax_delay())
                    .setRange(features.getRange()).setMeanOfLastThree(features.getMean_of_last_three())
                    .setDiffBetweenLastTwo(features.getDiff_between_last_two()).setSlopeDelay(features.getSlope_delay())
                    .build();

            ListenableFuture<PacketLossResponse> futureResponse = futureStub.predictPacketLoss(request);

            Futures.addCallback(futureResponse, new FutureCallback<>() {
                @Override
                public void onSuccess(PacketLossResponse response) {
                    boolean prediction = response.getHasPacketLoss();
                    logger.info("✅ 异步 gRPC 调用成功！预测结果: {}", prediction ? "可能丢包" : "正常");

                    PredictionResultDto resultDto = PredictionResultDto.builder()
                            .deviceId(features.getDeviceId())
                            .predictionTimestamp(System.currentTimeMillis())
                            .hasPacketLoss(prediction)
                            .build();

                    producerService.sendPredictionResult(resultDto);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("❌ 异步 gRPC 服务调用失败: {}", t.getMessage());
                }
            }, grpcCallbackExecutor);

        } catch (Exception e) {
            logger.error("❌ 构建 gRPC 请求时发生同步错误: {}", e.getMessage());
        }
    }

    @PreDestroy
    private void shutdown() throws InterruptedException {
        // 在应用关闭时，Spring会自动关闭我们定义的Bean，无需手动关闭线程池
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}

