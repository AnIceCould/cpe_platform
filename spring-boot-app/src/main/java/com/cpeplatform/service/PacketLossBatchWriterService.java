package com.cpeplatform.service;

import com.cpeplatform.adapter.kafka.PacketLossPersistenceConsumer;
import com.cpeplatform.persistence.entity.PacketLossEvent;
import com.cpeplatform.persistence.repository.PacketLossEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 负责定期将Redis中缓冲的丢包事件批量写入MySQL数据库。
 */
@Service
public class PacketLossBatchWriterService {

    private static final Logger logger = LoggerFactory.getLogger(PacketLossBatchWriterService.class);

    private final RedisTemplate<String, Object> redisTemplate;
    private final PacketLossEventRepository repository;

    public PacketLossBatchWriterService(RedisTemplate<String, Object> redisTemplate, PacketLossEventRepository repository) {
        this.redisTemplate = redisTemplate;
        this.repository = repository;
    }

    /**
     * 定时任务，每隔5秒执行一次。
     * fixedRate = 5000 表示无论上次任务执行多久，下次任务都会在上个任务开始后的5秒后启动。
     */
    @Scheduled(fixedRate = 5000)
    @Transactional // 确保整个批量写入操作在一个数据库事务中完成
    public void persistBufferedEvents() {
        // 1. 从Redis列表中获取所有当前缓冲的事件
        //    opsForList().range(key, 0, -1) 表示获取列表中的所有元素
        String bufferKey = PacketLossPersistenceConsumer.PACKET_LOSS_BUFFER_KEY;
        List<Object> bufferedObjects = redisTemplate.opsForList().range(bufferKey, 0, -1);

        // 2. 如果没有需要处理的事件，则直接返回
        if (bufferedObjects == null || bufferedObjects.isEmpty()) {
            return;
        }

        // 3. 在获取数据后，立即从Redis中移除这些数据，这是一个原子的“弹出全部”操作
        redisTemplate.delete(bufferKey);

        // 类型转换
        @SuppressWarnings("unchecked")
        List<PacketLossEvent> eventsToPersist = (List<PacketLossEvent>)(List<?>) bufferedObjects;

        logger.info("⏰ 定时任务触发: 发现 {} 条缓冲的丢包事件，准备批量写入MySQL...", eventsToPersist.size());

        try {
            // 4. 使用 saveAll 方法进行高效的批量插入
            repository.saveAll(eventsToPersist);
            logger.info("✅ 批量写入成功！ {} 条事件已持久化到数据库。", eventsToPersist.size());
        } catch (Exception e) {
            logger.error("❌ 批量写入MySQL时发生严重错误！本次批次的 {} 条数据可能已丢失。", eventsToPersist.size(), e);
        }
    }
}
