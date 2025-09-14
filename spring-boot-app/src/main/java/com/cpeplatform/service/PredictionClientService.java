package com.cpeplatform.service;

import com.cpeplatform.grpc.PacketLossRequest;
import com.cpeplatform.grpc.PacketLossResponse;
import com.cpeplatform.grpc.PredictionServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    /**
     * 在Bean初始化后，创建gRPC连接。
     */
    @PostConstruct
    private void init() {
        logger.info("正在连接到 gRPC 服务器，地址: {}:{}", grpcServerAddress, grpcServerPort);
        channel = ManagedChannelBuilder.forAddress(grpcServerAddress, grpcServerPort)
                .usePlaintext() // 使用非加密连接，开发环境适用
                .build();
        blockingStub = PredictionServiceGrpc.newBlockingStub(channel);
    }

    /**
     * 调用 gRPC 服务进行丢包预测。
     *
     * @param rtts 包含5个RTT值的列表
     * @return 预测结果，true表示可能丢包，false表示正常
     */
    public boolean predict(List<Integer> rtts) {
        if (rtts == null || rtts.isEmpty()) {
            logger.warn("输入的RTT列表为空，无法进行预测。");
            return false;
        }

        try {
            logger.info("准备调用 gRPC 预测服务，输入 RTTs: {}", rtts);
            // 1. 构建 gRPC 请求对象
            PacketLossRequest request = PacketLossRequest.newBuilder()
                    .addAllRtts(rtts)
                    .build();

            // 2. 发送请求并获取响应 (这是一个阻塞调用)
            PacketLossResponse response = blockingStub.predictPacketLoss(request);

            // 3. 从响应中提取结果
            boolean prediction = response.getHasPacketLoss();
            logger.info("✅ gRPC 服务调用成功！预测结果: {}", prediction ? "可能丢包" : "正常");
            return prediction;

        } catch (Exception e) {
            logger.error("❌ gRPC 服务调用失败: {}", e.getMessage());
            // 在生产环境中，这里应该有更复杂的错误处理逻辑
            return false;
        }
    }

    /**
     * 在应用关闭前，优雅地关闭gRPC连接。
     */
    @PreDestroy
    private void shutdown() throws InterruptedException {
        if (channel != null) {
            logger.info("正在关闭 gRPC 连接...");
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            logger.info("gRPC 连接已关闭。");
        }
    }
}
