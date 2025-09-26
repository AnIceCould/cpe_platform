package com.cpeplatform.service;

import com.cpeplatform.dto.PredictionResultDto;
import com.cpeplatform.persistence.entity.PacketLossEvent;
import com.cpeplatform.persistence.repository.PacketLossEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 一个专门的服务，其唯一职责是将丢包事件持久化到数据库。
 */
@Service
public class PacketLossPersistenceService {

    private static final Logger logger = LoggerFactory.getLogger(PacketLossPersistenceService.class);

    private final PacketLossEventRepository repository;

    public PacketLossPersistenceService(PacketLossEventRepository repository) {
        this.repository = repository;
    }

    /**
     * 将预测结果转换为实体并保存到 MySQL。
     * 这个操作是事务性的。
     * @param resultDto 从 Kafka 接收到的预测结果
     */
    @Transactional
    public void persistEvent(PredictionResultDto resultDto) {
        try {
            PacketLossEvent event = PacketLossEvent.builder()
                    .deviceId(resultDto.getDeviceId())
                    .eventTimestamp(resultDto.getPredictionTimestamp())
                    .hasPacketLoss(true)
                    .build();

            repository.save(event);

            logger.info("✅ 丢包事件已成功持久化到 MySQL。设备ID: {}", resultDto.getDeviceId());

        } catch (Exception e) {
            logger.error("❌ 持久化丢包事件到 MySQL 时发生错误。设备ID: {}", resultDto.getDeviceId(), e);
            // 重新抛出异常以确保事务能够回滚
            throw new RuntimeException("数据库持久化失败", e);
        }
    }
}
