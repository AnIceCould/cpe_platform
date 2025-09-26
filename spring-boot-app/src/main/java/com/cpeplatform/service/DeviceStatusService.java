package com.cpeplatform.service;

import com.cpeplatform.dto.CpeStatusDataDto;
import com.cpeplatform.persistence.repository.DeviceStatusRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 负责处理设备状态的业务逻辑
 */
@Service
public class DeviceStatusService {

    private static final Logger logger = LoggerFactory.getLogger(DeviceStatusService.class);

    private final DeviceStatusRepository repository;

    public DeviceStatusService(DeviceStatusRepository repository) {
        this.repository = repository;
    }

    /**
     * @param statusDto 从Kafka接收到的状态数据
     */
    public void updateDeviceStatus(CpeStatusDataDto statusDto) {
        try {
            logger.debug("准备对设备 [{}] 执行UPSERT操作，新状态: {}", statusDto.getDeviceId(), statusDto.getStatus());
            repository.upsertStatus(
                    statusDto.getDeviceId(),
                    statusDto.getStatus(),
                    statusDto.getTimestamp()
            );
            logger.info("✅ 设备 [{}] 状态UPSERT操作成功。", statusDto.getDeviceId());
        } catch (Exception e) {
            logger.error("❌ 对设备 [{}] 执行UPSERT时发生错误", statusDto.getDeviceId(), e);
        }
    }
}
