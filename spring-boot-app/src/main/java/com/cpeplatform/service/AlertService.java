package com.cpeplatform.service;

import com.cpeplatform.adapter.websocket.AlertWebsocketHandler;
import com.cpeplatform.config.ExecutorConfig;
import com.cpeplatform.dto.PredictionResultDto;
import com.cpeplatform.persistence.entity.PacketLossEvent;
import com.cpeplatform.persistence.repository.PacketLossEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 负责编排丢包警报的处理流程：先实时推送，再异步持久化。
 */
@Service
public class AlertService {

    private static final Logger logger = LoggerFactory.getLogger(AlertService.class);

    private final AlertWebsocketHandler websocketHandler;
    private final PacketLossEventRepository repository;

    public AlertService(AlertWebsocketHandler websocketHandler, PacketLossEventRepository repository) {
        this.websocketHandler = websocketHandler;
        this.repository = repository;
    }

    /**
     * 处理丢包警报的主方法。
     * @param resultDto 预测结果
     */
    public void processPacketLossAlert(PredictionResultDto resultDto) {
        // 步骤 1: 立即通过 WebSocket 推送警报 (同步执行)
        // 这是最高优先级的任务，因为它直接影响用户感知。
        websocketHandler.sendPacketLossAlert(resultDto);

        // 步骤 2: "发射后不管"，异步调用数据库持久化方法
        // 这个调用会立即返回，不会阻塞当前线程。
        persistAlertAsync(resultDto);
    }

    /**
     * 使用 @Async 注解，将这个方法的执行提交到我们指定的线程池中。
     * @param resultDto 预测结果
     */
    @Async(ExecutorConfig.DB_WRITER_EXECUTOR) // 指定使用我们创建的数据库写入线程池
    @Transactional
    public void persistAlertAsync(PredictionResultDto resultDto) {
        logger.info("...[后台线程] 开始持久化丢包事件: {}", resultDto.getDeviceId());
        try {
            PacketLossEvent event = PacketLossEvent.builder()
                    .deviceId(resultDto.getDeviceId())
                    .eventTimestamp(resultDto.getPredictionTimestamp())
                    .hasPacketLoss(true)
                    .build();

            repository.save(event);
            logger.info("✅ [后台线程] 丢包事件已成功保存到MySQL数据库！");
        } catch (Exception e) {
            // 在异步方法中，错误需要被显式地记录下来
            logger.error("❌ [后台线程] 异步保存丢包事件到数据库时发生错误", e);
        }
    }
}
