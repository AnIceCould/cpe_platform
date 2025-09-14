package com.cpeplatform.adapter.kafka;

import com.cpeplatform.dto.PredictionResultDto;
import com.cpeplatform.persistence.entity.PacketLossEvent;
import com.cpeplatform.persistence.repository.PacketLossEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 负责消费预测结果，并在检测到丢包时，将事件持久化到MySQL数据库。
 */
@Service
public class PacketLossPersistenceConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PacketLossPersistenceConsumer.class);

    private final PacketLossEventRepository repository;

    public PacketLossPersistenceConsumer(PacketLossEventRepository repository) {
        this.repository = repository;
    }

    /**
     * 监听 'cpe-prediction-result' 主题。
     * @param resultDto 从Kafka接收到的预测结果对象
     */
    // 【核心修改】: 指定使用我们刚刚创建的专属ContainerFactory
    @KafkaListener(topics = "${app.kafka.topic.prediction-result}",
            groupId = "${spring.kafka.consumer.group-id}-db-persister",
            containerFactory = "predictionResultKafkaListenerContainerFactory")
    public void consumeAndPersistPacketLoss(PredictionResultDto resultDto) {

        // 【核心条件判断】: 只处理预测结果为“丢包”的消息
        if (resultDto.isHasPacketLoss()) {

            logger.info("📬 检测到丢包事件，准备持久化到数据库... 设备ID: {}", resultDto.getDeviceId());

            // 1. 将接收到的 DTO 转换为数据库实体 (Entity)
            PacketLossEvent event = PacketLossEvent.builder()
                    .deviceId(resultDto.getDeviceId())
                    .eventTimestamp(resultDto.getPredictionTimestamp())
                    .hasPacketLoss(true) // 显式设置为 true
                    .build();

            try {
                // 2. 使用仓库的 save 方法将实体保存到数据库
                repository.save(event);
                logger.info("✅ 丢包事件已成功保存到MySQL数据库！");
            } catch (Exception e) {
                logger.error("❌ 保存丢包事件到数据库时发生错误", e);
                // 在生产环境中，这里可以加入重试逻辑或将失败的消息发送到“死信队列”
            }
        } else {
            // 如果不是丢包事件，可以选择性地打印一条debug日志或直接忽略
            logger.debug("接收到正常事件，无需持久化。设备ID: {}", resultDto.getDeviceId());
        }
    }
}
