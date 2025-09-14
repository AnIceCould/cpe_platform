package com.cpeplatform.flink;

import com.cpeplatform.flink.deserializer.CpeRawDataDeserializer;
import com.cpeplatform.flink.model.*;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CpeDataSplittingJob {

    // --- 配置常量 ---
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INPUT_TOPIC = "cpe-raw-data";
    // 【修改】新的输出 Topic 名称
    private static final String OUTPUT_FEATURES_TOPIC = "cpe-features-for-prediction";
    private static final String CONSUMER_GROUP_ID = "cpe-flink-processor-group";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<CpeRawData> source = KafkaSource.<CpeRawData>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId(CONSUMER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new CpeRawDataDeserializer()))
                .build();

        DataStream<CpeRawData> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Raw Data Source")
                .filter(java.util.Objects::nonNull);

        final OutputTag<CpeLatencyData> latencyTag = new OutputTag<CpeLatencyData>("latency-output"){};

        SingleOutputStreamOperator<CpeStatusData> mainStream = rawStream
                .process(new ProcessFunction<CpeRawData, CpeStatusData>() {
                    @Override
                    public void processElement(CpeRawData rawData, Context ctx, Collector<CpeStatusData> out) {
                        // 状态数据流保持不变 (如果需要)
                        // CpeStatusData statusData = new CpeStatusData(...);
                        // out.collect(statusData);

                        CpeLatencyData latencyData = new CpeLatencyData(rawData.getDeviceId(), rawData.getRtt(), rawData.getTimestamp());
                        ctx.output(latencyTag, latencyData);
                    }
                });

        DataStream<CpeLatencyData> latencyStream = mainStream.getSideOutput(latencyTag);

        // ------------------------------------------------------------------
        // 【【【 全新的特征工程逻辑 】】】
        // ------------------------------------------------------------------
        DataStream<CpeFeatures> featuresStream = latencyStream
                .keyBy(CpeLatencyData::getDeviceId)
                .countWindow(5)
                .process(new FeatureEngineeringProcessor()); // 使用新的处理器

        // 将计算好的特征集写入新的 Kafka Topic
        featuresStream.sinkTo(createKafkaSink(OUTPUT_FEATURES_TOPIC))
                .name("Features Kafka Sink");

        env.execute("CPE 实时特征工程作业");
    }

    /**
     * 核心处理器，负责从5个RTT数据点中计算出所有统计和趋势特征。
     */
    public static class FeatureEngineeringProcessor extends ProcessWindowFunction<CpeLatencyData, CpeFeatures, String, GlobalWindow> {
        @Override
        public void process(String deviceId, Context context, Iterable<CpeLatencyData> elements, Collector<CpeFeatures> out) {

            List<Integer> rtts = new ArrayList<>();
            for(CpeLatencyData element : elements) {
                rtts.add(element.getRtt());
            }

            if (rtts.size() != 5) return; // 安全校验

            // --- 开始计算特征 ---
            double mean = rtts.stream().mapToInt(Integer::intValue).average().orElse(0.0);
            double min = Collections.min(rtts);
            double max = Collections.max(rtts);

            List<Integer> sortedRtts = new ArrayList<>(rtts);
            Collections.sort(sortedRtts);
            double median = sortedRtts.get(2); // 5个点的中位数是第3个

            double range = max - min;
            double meanLastThree = rtts.subList(2, 5).stream().mapToInt(Integer::intValue).average().orElse(0.0);
            double diffLastTwo = rtts.get(4) - rtts.get(3);

            // 计算斜率 (简单线性回归)
            double slope = calculateSlope(rtts);

            // 构建特征对象
            CpeFeatures features = CpeFeatures.builder()
                    .deviceId(deviceId)
                    .aggregationTimestamp(System.currentTimeMillis())
                    .delay_1(rtts.get(0)).delay_2(rtts.get(1)).delay_3(rtts.get(2)).delay_4(rtts.get(3)).delay_5(rtts.get(4))
                    .mean_delay(mean)
                    .min_delay(min)
                    .mid_delay(median)
                    .max_delay(max)
                    .range(range)
                    .mean_of_last_three(meanLastThree)
                    .diff_between_last_two(diffLastTwo)
                    .slope_delay(slope)
                    .build();

            out.collect(features);
        }

        /**
         * 计算5个点的简单线性回归斜率
         */
        private double calculateSlope(List<Integer> y) {
            int n = y.size();
            double[] x = IntStream.range(1, n + 1).mapToDouble(i -> i).toArray();

            double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
            for(int i = 0; i < n; i++) {
                sumX += x[i];
                sumY += y.get(i);
                sumXY += x[i] * y.get(i);
                sumX2 += x[i] * x[i];
            }

            if ((n * sumX2 - sumX * sumX) == 0) return 0; // 避免除以零

            return (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        }
    }

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

