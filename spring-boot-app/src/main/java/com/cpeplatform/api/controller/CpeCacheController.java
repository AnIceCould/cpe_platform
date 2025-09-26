package com.cpeplatform.api.controller;

import com.cpeplatform.service.CpeCacheService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 提供用于管理CPE设备缓存的RESTful API端点。
 */
@RestController
@RequestMapping("/api/cpe/cache") // 所有此控制器中的URL都将以 /api/cpe/cache 开头
public class CpeCacheController {

    private final CpeCacheService cpeCacheService;

    public CpeCacheController(CpeCacheService cpeCacheService) {
        this.cpeCacheService = cpeCacheService;
    }

    /**
     * 处理删除指定设备最新丢包事件缓存的DELETE请求。
     *
     * @param deviceId 从URL路径中获取的设备ID
     * @return 返回一个HTTP响应
     */
    @DeleteMapping("/packetloss/{deviceId}") // 端点URL示例: DELETE http://localhost:8080/api/cpe/cache/packetloss/cpe-device-001
    public ResponseEntity<String> deletePacketLossCache(@PathVariable String deviceId) {
        // 校验输入
        if (deviceId == null || deviceId.isBlank()) {
            return ResponseEntity.badRequest().body("设备ID (deviceId) 不能为空。");
        }

        // 调用服务层来执行删除逻辑
        cpeCacheService.clearPacketLossEvent(deviceId);

        // 返回一个成功的HTTP响应
        // ResponseEntity.ok() 会返回 200 OK 状态码
        return ResponseEntity.ok("已成功清除设备 [" + deviceId + "] 的丢包事件缓存。");
    }
}
