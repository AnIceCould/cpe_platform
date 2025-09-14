package com.cpeplatform.flink;

import com.cpeplatform.flink.deserializer.CpeRawDataDeserializer;
import com.cpeplatform.flink.model.*; // 导入所有模型
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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink流处理作业的主类。
 * 职责：
 * 1. 从 "cpe-raw-data" 主题消费原始数据。
 * 2. 将每条消息拆分为 "延迟" 和 "状态" 两条消息，并发送到Kafka。
 * 3. 【新增】对延迟数据流进行聚合，每5条相同ID的数据打包成一条消息，发送到新的Kafka Topic。
 */
public class CpeDataSplittingJob {

    // --- 配置常量 ---
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INPUT_TOPIC = "cpe-raw-data";
    private static final String OUTPUT_LATENCY_TOPIC = "cpe-processed-latency";
    private static final String OUTPUT_STATUS_TOPIC = "cpe-processed-status";
    // 新增：丢包聚合数据的输出Topic
    private static final String OUTPUT_PACKETLOSS_AGGREGATION_TOPIC = "cpe-packetloss-aggregation";
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

        // 步骤 7: 将拆分后的状态流和原始延迟流写入 Kafka (原有逻辑)
        statusStream.sinkTo(createKafkaSink(OUTPUT_STATUS_TOPIC)).name("Status Kafka Sink");
        latencyStream.sinkTo(createKafkaSink(OUTPUT_LATENCY_TOPIC)).name("Latency Kafka Sink");

        // ------------------------------------------------------------------
        // 【【【 新增的聚合处理逻辑 】】】
        // ------------------------------------------------------------------

        // 步骤 8: 对延迟数据流进行聚合
        DataStream<CpePacketLossAggregation> packetLossStream = latencyStream
                // 8.1: 按照 deviceId 对数据进行分组
                .keyBy(CpeLatencyData::getDeviceId)
                // 8.2: 创建一个计数窗口，窗口大小为5
                .countWindow(5)
                // 8.3: 对窗口内的数据进行处理，打包成一个新的对象
                .process(new PacketLossAggregator());

        // 步骤 9: 将聚合后的数据流写入新的 Kafka Topic
        packetLossStream.sinkTo(createKafkaSink(OUTPUT_PACKETLOSS_AGGREGATION_TOPIC))
                .name("Packet Loss Aggregation Kafka Sink");


        // 步骤 10: 触发作业执行 (作业名更新)
        env.execute("CPE 数据实时处理与聚合作业");
    }

    /**
     * Flink 的 ProcessWindowFunction，用于处理一个已触发的窗口中的所有数据。
     */
    public static class PacketLossAggregator extends ProcessWindowFunction<CpeLatencyData, CpePacketLossAggregation, String, GlobalWindow> {
        @Override
        public void process(String deviceId, Context context, Iterable<CpeLatencyData> elements, Collector<CpePacketLossAggregation> out) {
            List<LatencyDataPoint> points = new ArrayList<>();
            for (CpeLatencyData element : elements) {
                points.add(new LatencyDataPoint(element.getRtt(), element.getTimestamp()));
            }

            // 创建最终的聚合结果对象
            CpePacketLossAggregation aggregation = new CpePacketLossAggregation(deviceId, System.currentTimeMillis(), points);

            // 发送聚合结果到下游
            out.collect(aggregation);
        }
    }

    /**
     * 创建一个通用的 Kafka Sink，用于将数据写入指定的 Topic。
     * @param topic 目标 Kafka 主题
     * @param <T>   要发送的数据类型
     * @return 配置好的 KafkaSink 实例
     */
    private static <T> KafkaSink<T> createKafkaSink(String topic) {
        KafkaRecordSerializationSchema<T> serializer = KafkaRecordSerializationSchema.<T>builder()
                .setTopic(topic)
                .setValueSerializationSchema(new JsonSerializationSchema<>())
                .build();

        return KafkaSink.<T>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(serializer)
                .build();
    }
}

