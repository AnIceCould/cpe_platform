package com.cpeplatform.adapter.kafka;

import com.cpeplatform.dto.CpeStatusDataDto;
import com.cpeplatform.service.DeviceStatusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 负责消费设备状态消息，并将其委托给 DeviceStatusService 进行处理。
 */
@Service
public class DeviceStatusConsumer {

    private static final Logger logger = LoggerFactory.getLogger(DeviceStatusConsumer.class);

    private final DeviceStatusService deviceStatusService;

    public DeviceStatusConsumer(DeviceStatusService deviceStatusService) {
        this.deviceStatusService = deviceStatusService;
    }

    @KafkaListener(topics = "${app.kafka.topic.processed-status}",
            groupId = "${spring.kafka.consumer.group-id}-status-persister",
            containerFactory = "statusKafkaListenerContainerFactory") // 使用专属的Factory
    public void consumeDeviceStatus(CpeStatusDataDto statusDto) {
        logger.debug("接收到设备状态消息: {}", statusDto);
        // 将业务逻辑完全委托给Service层处理
        deviceStatusService.updateDeviceStatus(statusDto);
    }
}
