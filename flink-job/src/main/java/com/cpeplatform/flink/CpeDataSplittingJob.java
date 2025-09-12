package com.cpeplatform.flink;

import com.cpeplatform.flink.deserializer.CpeRawDataDeserializer;
import com.cpeplatform.flink.model.CpeLatencyData;
import com.cpeplatform.flink.model.CpeRawData;
import com.cpeplatform.flink.model.CpeStatusData;
import com.cpeplatform.flink.serializer.JsonSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Flink流处理作业的主类（最终修正版）。
 * 职责：
 * 1. 从 "cpe-raw-data" 主题消费原始数据。
 * 2. 将每条消息拆分为 "延迟" 和 "状态" 两条消息。
 * 3. 将拆分后的消息分别发送到 "cpe-processed-latency" 和 "cpe-processed-status" 主题。
 */
public class CpeDataSplittingJob {

    // --- 配置常量 ---
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INPUT_TOPIC = "cpe-raw-data";
    private static final String OUTPUT_LATENCY_TOPIC = "cpe-processed-latency";
    private static final String OUTPUT_STATUS_TOPIC = "cpe-processed-status";
    private static final String CONSUMER_GROUP_ID = "cpe-flink-processor-group";

    public static void main(String[] args) throws Exception {

        // 步骤 1: 初始化 Flink 流处理执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 步骤 2: 定义 Kafka Source (数据从哪里来)
        KafkaSource<CpeRawData> source = KafkaSource.<CpeRawData>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId(CONSUMER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new CpeRawDataDeserializer()))
                .build();

        // 步骤 3: 从 Source 创建数据流，并过滤掉坏数据
        DataStream<CpeRawData> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Raw Data Source")
                .filter(java.util.Objects::nonNull);

        // 步骤 4: 定义“侧输出”标签
        final OutputTag<CpeLatencyData> latencyTag = new OutputTag<CpeLatencyData>("latency-output"){};

        // 步骤 5: 核心处理逻辑 - 拆分数据流
        SingleOutputStreamOperator<CpeStatusData> mainStream = rawStream
                .process(new ProcessFunction<CpeRawData, CpeStatusData>() {
                    @Override
                    public void processElement(CpeRawData rawData, Context ctx, Collector<CpeStatusData> out) {
                        CpeStatusData statusData = new CpeStatusData(rawData.getDeviceId(), rawData.getStatus(), rawData.getTimestamp());
                        out.collect(statusData);

                        CpeLatencyData latencyData = new CpeLatencyData(rawData.getDeviceId(), rawData.getRtt(), rawData.getTimestamp());
                        ctx.output(latencyTag, latencyData);
                    }
                });

        // 步骤 6: 获取拆分后的两条数据流
        DataStream<CpeStatusData> statusStream = mainStream;
        DataStream<CpeLatencyData> latencyStream = mainStream.getSideOutput(latencyTag);

        // 步骤 7: 定义 Kafka Sink (数据到哪里去)
        KafkaSink<CpeStatusData> statusSink = createKafkaSink(OUTPUT_STATUS_TOPIC);
        KafkaSink<CpeLatencyData> latencySink = createKafkaSink(OUTPUT_LATENCY_TOPIC);

        // 步骤 8: 将数据流写入 Kafka
        statusStream.sinkTo(statusSink).name("Status Kafka Sink");
        latencyStream.sinkTo(latencySink).name("Latency Kafka Sink");

        // 步骤 9: 触发作业执行
        env.execute("CPE 数据实时拆分作业");
    }

    /**
     * 创建一个通用的 Kafka Sink，用于将数据写入指定的 Topic。
     * @param topic 目标 Kafka 主题
     * @param <T>   要发送的数据类型
     * @return 配置好的 KafkaSink 实例
     */
    private static <T> KafkaSink<T> createKafkaSink(String topic) {
        // 【核心修正】显式指定泛型T，解决类型推断问题
        // 我们需要先创建一个正确类型的 KafkaRecordSerializationSchema 实例
        KafkaRecordSerializationSchema<T> serializer = KafkaRecordSerializationSchema.<T>builder()
                .setTopic(topic)
                .setValueSerializationSchema(new JsonSerializationSchema<>())
                .build();

        // 然后再构建 KafkaSink
        return KafkaSink.<T>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(serializer)
                .build();
    }
}

