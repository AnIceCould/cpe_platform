package com.cpeplatform.adapter.kafka;

import com.cpeplatform.api.dto.CpeRawDataDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Kafka生产者模拟器.
 * <p>
 * 该类实现了CommandLineRunner接口，这使得它的run方法会在Spring Boot应用完全启动后被自动执行。
 * 它会创建一个独立的后台线程，用于持续不断地生成并向Kafka主题发送模拟的CPE设备数据。
 */
@Component // 将该类注册为Spring容器中的一个组件(Bean)
public class KafkaDataSimulator implements CommandLineRunner {

    // 使用SLF4J的LoggerFactory获取Logger实例
    private static final Logger logger = LoggerFactory.getLogger(KafkaDataSimulator.class);

    // KafkaTemplate是Spring封装的Kafka生产者客户端，用于方便地发送消息
    private final KafkaTemplate<String, String> kafkaTemplate;
    // Jackson库的核心组件，用于Java对象和JSON字符串之间的转换
    private final ObjectMapper objectMapper;

    // 使用@Value注解从application.yml配置文件中注入要发送的Topic名称
    @Value("${app.kafka.topic.raw-data}")
    private String rawDataTopic;

    // 从application.yml中注入一个布尔值，用于控制是否启用模拟器，默认为true
    @Value("${app.simulator.enabled:true}")
    private boolean simulatorEnabled;

    /**
     * 构造函数注入依赖。
     * Spring会自动将容器中配置好的KafkaTemplate和ObjectMapper实例传递进来。
     * @param kafkaTemplate Kafka生产者模板
     * @param objectMapper Jackson JSON处理工具
     */
    public KafkaDataSimulator(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * CommandLineRunner接口的实现方法，在应用启动后执行。
     * @param args 命令行参数
     */
    @Override
    public void run(String... args) {
        // 检查模拟器是否已在配置文件中禁用
        if (!simulatorEnabled) {
            // 使用 SLF4J Logger 实例来记录中文日志
            logger.info("Kafka数据模拟器已禁用。");
            return;
        }

        logger.info("启动Kafka数据模拟器... 正在向Topic发送数据: {}", rawDataTopic);
        // 创建并启动一个新线程来运行模拟器，这样可以避免阻塞Spring Boot应用的主启动流程
        new Thread(this::runSimulator).start();
    }

    /**
     * 模拟器核心运行逻辑。
     * 在一个无限循环中生成数据并发送。
     */
    private void runSimulator() {
        Random random = new Random();
        // 预设一些模拟用的设备ID
        List<String> deviceIds = Arrays.asList(
                "cpe-device-001", "cpe-device-002", "cpe-device-003",
                "cpe-device-004", "cpe-device-005"
        );
        // 预设一些模拟用的设备状态
        List<String> statuses = Arrays.asList("ONLINE", "ONLINE", "ONLINE", "DEGRADED", "OFFLINE");

        try {
            // 启动一个无限循环，除非线程被中断
            while (!Thread.currentThread().isInterrupted()) {
                // 1. 从列表中随机选择一个设备ID
                String deviceId = deviceIds.get(random.nextInt(deviceIds.size()));

                // 2. 使用Builder模式构建模拟数据对象
                CpeRawDataDto rawData = CpeRawDataDto.builder()
                        .deviceId(deviceId)
                        .status(statuses.get(random.nextInt(statuses.size())))
                        .rtt(10 + random.nextInt(90)) // 生成10到99之间的随机往返时延
                        .timestamp(System.currentTimeMillis()) // 使用当前系统时间作为时间戳
                        .build();

                // 3. 将CpeRawDataDto对象序列化为JSON格式的字符串
                String messagePayload = objectMapper.writeValueAsString(rawData);

                // 4. 调用KafkaTemplate的send方法发送消息
                //    第一个参数是Topic名称
                //    第二个参数是消息的Key（这里用deviceId），确保相同设备的消息进入同一分区
                //    第三个参数是消息体（Payload）
                kafkaTemplate.send(rawDataTopic, rawData.getDeviceId(), messagePayload);

                // 使用 SLF4J Logger 实例来记录中文日志 (带参数)
                logger.info("已发送消息: {}", messagePayload);

                // 5. 暂停1秒，控制发送频率
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (InterruptedException e) {
            // 当线程在sleep时被中断，会抛出此异常
            logger.warn("Kafka数据模拟器被中断。");
            // 重新设置中断状态，以便上层代码能够感知到
            Thread.currentThread().interrupt();
        } catch (JsonProcessingException e) {
            // 当DTO对象序列化为JSON失败时，会抛出此异常
            logger.error("将DTO序列化为JSON时出错", e);
        }
    }
}

