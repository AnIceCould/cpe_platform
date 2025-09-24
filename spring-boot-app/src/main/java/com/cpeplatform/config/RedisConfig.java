package com.cpeplatform.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Redis 的核心配置类。
 * 负责定义和配置 RedisTemplate Bean。
 */
@Configuration
public class RedisConfig {

    /**
     * 定义一个我们项目中需要的 RedisTemplate<String, Object> Bean。
     * Spring Boot 会自动找到这个 Bean 并将其注入到需要它的地方
     *
     * @param redisConnectionFactory Spring Boot 自动配置好的 Redis 连接工厂
     * @return 配置好的 RedisTemplate 实例
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        // 1. 创建 RedisTemplate 对象
        RedisTemplate<String, Object> template = new RedisTemplate<>();

        // 2. 设置连接工厂
        template.setConnectionFactory(redisConnectionFactory);

        // 3. 配置序列化器
        //    - Key 序列化器: 我们使用 String 格式
        //    - Value 序列化器: 我们使用通用的 Jackson JSON 格式，它可以处理任何Java对象
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());

        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());

        // 4. 初始化 RedisTemplate
        template.afterPropertiesSet();

        // 5. 返回配置好的实例
        return template;
    }
}
