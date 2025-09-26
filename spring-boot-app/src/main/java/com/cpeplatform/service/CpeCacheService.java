package com.cpeplatform.service;

import com.cpeplatform.dto.PredictionResultDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

/**
 * 专门负责管理CPE设备在Redis中的缓存数据。
 */
@Service
public class CpeCacheService {

    private static final Logger logger = LoggerFactory.getLogger(CpeCacheService.class);

    // 定义用于存储所有设备最新丢包事件的Redis Hash的Key
    public static final String LATEST_PACKETLOSS_KEY = "cpe:latest_packetloss_events";

    private final RedisTemplate<String, Object> redisTemplate;

    public CpeCacheService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 更新或创建指定设备的最新丢包事件缓存。
     *
     * @param resultDto 包含最新丢包事件信息的DTO
     */
    public void updateLatestPacketLossEvent(PredictionResultDto resultDto) {
        if (resultDto == null || resultDto.getDeviceId() == null) {
            return;
        }
        try {
            // 使用 HSET 命令：在名为 LATEST_PACKETLOSS_KEY 的 Hash 中，
            // 设置一个字段（key为deviceId），其值为resultDto对象（会被自动序列化为JSON）。
            // 如果字段已存在，则会覆盖更新。
            redisTemplate.opsForHash().put(LATEST_PACKETLOSS_KEY, resultDto.getDeviceId(), resultDto);
            logger.info("✅ 已更新Redis中设备 [{}] 的最新丢包事件缓存。", resultDto.getDeviceId());
        } catch (Exception e) {
            logger.error("❌ 更新Redis设备 [{}] 缓存时出错。", resultDto.getDeviceId(), e);
        }
    }

    /**
     * 当一个设备的丢包状态恢复正常时，可以从缓存中移除它的丢包记录。
     * (这是一个可选的辅助方法)
     *
     * @param deviceId 设备ID
     */
    public void clearPacketLossEvent(String deviceId) {
        if (deviceId == null) {
            return;
        }
        try {
            // 使用 HDEL 命令：从 Hash 中删除指定的字段。
            redisTemplate.opsForHash().delete(LATEST_PACKETLOSS_KEY, deviceId);
            logger.info("✅ 已清除Redis中设备 [{}] 的丢包事件缓存。", deviceId);
        } catch (Exception e) {
            logger.error("❌ 清除Redis设备 [{}] 缓存时出错。", deviceId, e);
        }
    }
}
