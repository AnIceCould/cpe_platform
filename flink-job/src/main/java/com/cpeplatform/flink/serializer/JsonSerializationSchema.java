package com.cpeplatform.flink.serializer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一个通用的JSON序列化器，可以将任何Java对象 (POJO) 序列化为JSON格式的字节数组。
 * @param <T> 要序列化的对象类型
 */
public class JsonSerializationSchema<T> implements SerializationSchema<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonSerializationSchema.class);
    // ObjectMapper是线程安全的，可以作为静态成员变量重用，以提高性能。
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(T element) {
        try {
            // 使用Jackson将Java对象转换为JSON格式的字节数组
            return objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            // 如果序列化失败，记录错误日志
            LOG.error("序列化对象失败: {}", element, e);
        }
        // 发生异常时返回空数组，避免因单个坏数据导致整个作业失败
        return new byte[0];
    }
}

