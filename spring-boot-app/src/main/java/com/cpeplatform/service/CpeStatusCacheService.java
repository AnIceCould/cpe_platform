package com.cpeplatform.service;

import com.cpeplatform.persistence.entity.DeviceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

/**
 * 专门负责管理CPE设备状态在Redis中的缓存。
 */
@Service
public class CpeStatusCacheService {

    private static final Logger logger = LoggerFactory.getLogger(CpeStatusCacheService.class);

    // 定义用于存储所有设备最新状态的Redis Hash的Key
    public static final String DEVICE_STATUSES_KEY = "device:statuses";

    private final RedisTemplate<String, Object> redisTemplate;

    public CpeStatusCacheService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 更新或创建指定设备的状态缓存。
     *
     * @param deviceStatus 包含最新状态的实体对象
     */
    public void updateStatus(DeviceStatus deviceStatus) {
        if (deviceStatus == null || deviceStatus.getDeviceId() == null) {
            return;
        }
        try {
            // 使用 HSET 命令更新 Hash 中的一个字段
            redisTemplate.opsForHash().put(DEVICE_STATUSES_KEY, deviceStatus.getDeviceId(), deviceStatus);
            logger.debug(" -> Redis缓存已更新: 设备 [{}], 状态 [{}]", deviceStatus.getDeviceId(), deviceStatus.getStatus());
        } catch (Exception e) {
            logger.error("❌ 更新Redis设备 [{}] 缓存时出错。", deviceStatus.getDeviceId(), e);
            // 抛出运行时异常，以便上层 @Transactional 能够捕获并触发回滚
            throw new RuntimeException("更新Redis缓存失败", e);
        }
    }
}
