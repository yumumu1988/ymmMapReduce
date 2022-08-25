package com.example.task;

import lombok.Data;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @author zhanghailin
 * @date 2022/7/5
 *
 * mapreduce模型子任务类。用于执行子任务。
 * 主要属性包含：子任务结果集result、子任务参数params、子任务执行计数器unfinishedTaskCount、全部子任务完成信号量allTaskFinished、
 * 任务执行器axxMapReduceTask。
 */
@Data
public class YmmMapReduceSubTask<P, T> implements Runnable {

    private List<T> result;
    private P params;
    private Semaphore allTaskFinished;
    private AtomicInteger unfinishedTaskCount;
    private YmmMapReduceTask ymmMapReduceTask;

    private Function<P, List<T>> function;

    /**
     *  构造函数。
     * @param params 子任务参数 (P：泛型)
     * @param function 子任务执行函数（T：泛型）
     */
    public YmmMapReduceSubTask(P params, Function<P, List<T>> function) {
        this.params = params;
        this.function = function;
    }

    /**
     *  初始化子任务
     * @param ymmMapReduceTask MapReduce任务（父级）对象。用于回调状态。
     * @param unfinishedTaskCount 子任务执行计数器
     * @param allTaskFinished 全部子任务完成信号量
     */
    public void initSubTask(YmmMapReduceTask ymmMapReduceTask, AtomicInteger unfinishedTaskCount, Semaphore allTaskFinished) {
        this.ymmMapReduceTask = ymmMapReduceTask;
        this.unfinishedTaskCount = unfinishedTaskCount;
        this.allTaskFinished = allTaskFinished;
    }

    @Override
    public void run() {
        try {
            ymmMapReduceTask.acquireOneThread();
            this.result = function.apply(params);
        } catch (Exception e) {
            ymmMapReduceTask.callback(YmmMapReduceErrorCode.ERROR_CODE_SUBTASK_FAILED, e);
        } finally {
            if (0 == unfinishedTaskCount.decrementAndGet() && allTaskFinished != null) {
                allTaskFinished.release();
            }
            ymmMapReduceTask.releaseOneThread();
        }
    }
}
