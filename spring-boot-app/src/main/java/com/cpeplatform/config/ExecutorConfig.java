package com.cpeplatform.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;

/**
 * 应用程序的线程池配置。
 */
@Configuration
public class ExecutorConfig {

    private static final Logger logger = LoggerFactory.getLogger(ExecutorConfig.class);

    public static final String GRPC_CALLBACK_EXECUTOR = "grpcCallbackExecutor";

    /**
     * 创建一个专门用于处理 gRPC 回调的线程池。
     * 这个线程池经过特殊配置，可以捕获并打印出在异步任务中被“吞噬”的异常。
     * @return ExecutorService 实例
     */
    @Bean(name = GRPC_CALLBACK_EXECUTOR)
    public ExecutorService grpcCallbackExecutor() {
        logger.info("正在创建 gRPC 回调专用线程池...");
        // 我们创建一个自定义的 ThreadPoolExecutor
        return new ThreadPoolExecutor(
                10, // 核心线程数
                20, // 最大线程数
                60L, TimeUnit.SECONDS, // 空闲线程存活时间
                new LinkedBlockingQueue<>()) { // 工作队列

            /**
             * 【【【核心修正】】】
             * 我们重写 afterExecute 方法。这个方法在每个任务执行完毕后都会被调用。
             * @param r 刚刚执行完的任务
             * @param t 任务执行期间抛出的异常 (如果任务正常完成，则为 null)
             */
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                // 检查任务是否是因为一个未捕获的异常而结束的
                if (t == null && r instanceof Future<?>) {
                    try {
                        Future<?> future = (Future<?>) r;
                        if (future.isDone()) {
                            // 调用 .get() 可以将任务内部的异常重新抛出
                            future.get();
                        }
                    } catch (CancellationException ce) {
                        // 任务被取消，是正常情况
                        t = ce;
                    } catch (ExecutionException ee) {
                        // 任务执行时，内部抛出了异常！
                        t = ee.getCause();
                    } catch (InterruptedException ie) {
                        // 当前线程被中断
                        Thread.currentThread().interrupt();
                    }
                }
                // 如果 t 不为 null，说明我们捕获到了一个被“吞噬”的异常！
                if (t != null) {
                    // 我们将它大声地打印到错误日志中
                    logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    logger.error("  捕获到 gRPC 回调线程池中的未处理异常！");
                    logger.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", t);
                }
            }
        };
    }
}
