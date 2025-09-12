package com.cpeplatform.flink.deserializer;

import com.cpeplatform.flink.model.CpeRawData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 自定义的Kafka消息反序列化器。
 * 它的作用是将从Kafka消费到的JSON格式的二进制消息 (byte[])，
 * 转换成 Flink 可以处理的 CpeRawData Java对象。
 */
public class CpeRawDataDeserializer implements DeserializationSchema<CpeRawData> {

    private static final Logger LOG = LoggerFactory.getLogger(CpeRawDataDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CpeRawData deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        try {
            // 使用Jackson将字节数组解析为CpeRawData对象
            return objectMapper.readValue(message, CpeRawData.class);
        } catch (Exception e) {
            // 如果JSON格式错误导致解析失败，打印错误日志并返回null
            LOG.error("反序列化消息失败: {}", new String(message), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(CpeRawData nextElement) {
        // 我们处理的是一个无限的Kafka流，所以永远没有结束
        return false;
    }

    @Override
    public TypeInformation<CpeRawData> getProducedType() {
        // 告诉Flink这个反序列化器产生的数据类型
        return TypeInformation.of(CpeRawData.class);
    }
}

