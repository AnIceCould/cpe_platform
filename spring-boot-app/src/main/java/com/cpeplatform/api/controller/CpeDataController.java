package com.cpeplatform.api.controller;

import com.cpeplatform.api.dto.CpeRawDataDto;
import com.cpeplatform.service.CpeDataService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 提供用于接收CPE数据的RESTful API端点。
 */
@RestController
@RequestMapping("/api/cpe") // 所有在此控制器中的URL都将以 /api/cpe 开头
public class CpeDataController {

    private final CpeDataService cpeDataService;

    /**
     * 构造函数注入CpeDataService。
     * @param cpeDataService 业务逻辑服务
     */
    public CpeDataController(CpeDataService cpeDataService) {
        this.cpeDataService = cpeDataService;
    }

    /**
     * 处理向系统提交CPE原始数据的POST请求。
     *
     * @param rawDataDto Spring会自动将请求体中的JSON数据反序列化为CpeRawDataDto对象
     * @return 返回一个HTTP响应
     */
    @PostMapping("/data") // 端点URL: POST http://localhost:8080/api/cpe/data
    public ResponseEntity<String> submitCpeData(@RequestBody CpeRawDataDto rawDataDto) {
        // 校验输入数据 (在实际应用中，这里应该有更完善的校验逻辑)
        if (rawDataDto.getDeviceId() == null || rawDataDto.getDeviceId().isEmpty()) {
            return ResponseEntity.badRequest().body("设备ID (deviceId) 不能为空。");
        }

        // 调用服务层处理业务逻辑
        cpeDataService.sendRawDataToKafka(rawDataDto);

        // 返回一个成功的HTTP响应
        return ResponseEntity.status(HttpStatus.ACCEPTED).body("数据已接收并正在处理中。");
    }
}
//```
//
//        ### 如何使用这个新接口
//
//1.  **启动您的Spring Boot应用**。
//        2.  使用任何API测试工具（如 Postman、curl 或 Insomnia）向以下地址发送一个 `POST` 请求：
//
//        **URL**: `http://localhost:8080/api/cpe/data`
//
//        **请求体 (Body)** (选择 `raw` 和 `JSON` 类型):
//        ```json
//{
//    "deviceId": "cpe-from-api-007",
//        "status": "ONLINE",
//        "rtt": 42,
//        "timestamp": 1757788800000
//}

