package com.cpeplatform;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * CPE平台服务主启动类
 *
 * &#064;SpringBootApplication  注解是一个组合注解，它包含了：
 * - @SpringBootConfiguration: 标记该类为Spring Boot的配置类。
 * - @EnableAutoConfiguration: 启用Spring Boot的自动配置机制。
 * - @ComponentScan: 自动扫描该类所在的包以及下级包中的组件。
 */
@SpringBootApplication
@EnableAsync
public class CpePlatformApplication {

    /**
     * Spring Boot应用程序的入口点。
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        SpringApplication.run(CpePlatformApplication.class, args);
    }

}
