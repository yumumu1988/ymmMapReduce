package com.example.task;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhanghailin
 * @date 2022/7/5
 *
 * mapreduce执行器。单例且所有任务共用。本质上是一个线程池对象。
 * 以下配置可以通过Apollo管理实时更新。
 * enable是否启用MapReduce执行模式。
 * coreThread核心线程数。
 * workingThreadCount最大工作线程数。用来控制并发量。
 */
@Component
@Slf4j
public class YmmMapReduceExecutor implements InitializingBean {

    @Value("${map.reduce.enable:false}")
    private boolean enable;
    @Value("${map.reduce.core.thread:50}")
    private Integer coreThread;
    @Value("${map.reduce.working.thread.threshold:400}")
    private Integer workingThreadCount;

    private ThreadPoolExecutor executor;

    private static AtomicInteger currentActiveThread = new AtomicInteger(0);

    protected static void acquireOneThread() {
        currentActiveThread.incrementAndGet();
    }

    protected static void releaseOneThread() {
        currentActiveThread.decrementAndGet();
    }

    protected void errorLog(String msg, Exception e) {
        log.error(msg, e);
    }

    protected void warnLog(String msg) {
        log.warn(msg);
    }

    protected void infoLog(String msg) {
        log.info(msg);
    }

    protected <T, P> void submit(YmmMapReduceSubTask<P, T> subTask) {
        executor.submit(subTask);
    }

    protected boolean enable() {
        return enable;
    }

    protected Integer getFreeThreads() {
        return Math.min(1000, workingThreadCount) - currentActiveThread.get();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        executor =  new ThreadPoolExecutor(coreThread, 1000,
                30L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>());
    }
}
