package com.example.task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @author zhanghailin
 * @date 2022/7/5
 *
 * mapreduc任务对象类。用于封装MapReduce模型。
 * 主要属性包含：map任务集合subTasks、map任务集合全部完成信号量allTaskFinished、任务执行计数器unfinishedTaskCount、
 * 执行异常状态throwException、任务参数defaultParams、默认执行（非MapReduce模式下的处理方法）函数defaultFunction、
 * MapReduce任务执行器axxMapReduceExecutor、map任务执行超时时间（超时后会操作默认执行函数）waitTimeOut。
 */
public class YmmMapReduceTask<D, P, T> {

    private List<YmmMapReduceSubTask<P, T>> subTasks;
    private final Semaphore allTaskFinished = new Semaphore(1);
    private AtomicInteger unfinishedTaskCount;
    private boolean throwException;
    private final Function<D, List<T>> defaultFunction;
    private final D defaultParams;
    private final YmmMapReduceExecutor ymmMapReduceExecutor;
    private Long waitTimeOut = 500L;

    /**
     *  mapreduce任务对象类构造函数。实例化过程中会判断是否以MapReduce模式执行任务，根据子任务映射函数拆分子任务集合。
     *  D：默认参数类型（泛型）；P：子任务参数类型（泛型）；T：返回值类型（泛型）。
     * @param defaultParams 任务执行默认参数
     * @param createSubTaskFunction 子任务映射函数。根据默认参数映射成多个子任务。
     * @param defaultFunction 默认执行函数。当MapReduce模式无法执行时的fallback执行函数。
     * @param ymmMapReduceExecutor 任务执行器
     * @param waitTimeOut 子任务超时时间。超时后会执行默认执行函数。
     */
    public YmmMapReduceTask(D defaultParams, Function<D, List<YmmMapReduceSubTask<P, T>>> createSubTaskFunction ,
                            Function<D, List<T>> defaultFunction, YmmMapReduceExecutor ymmMapReduceExecutor, Long waitTimeOut) {
        if (ymmMapReduceExecutor.enable() && ymmMapReduceExecutor.getFreeThreads() > 0) {
            this.subTasks = createSubTaskFunction.apply(defaultParams);
            unfinishedTaskCount = new AtomicInteger(subTasks.size());
            if (null != waitTimeOut) {
                this.waitTimeOut = waitTimeOut;
            }
        }
        this.ymmMapReduceExecutor = ymmMapReduceExecutor;
        this.defaultFunction = defaultFunction;
        this.defaultParams = defaultParams;
    }


    public YmmMapReduceTask(D defaultParams, Function<D, List<YmmMapReduceSubTask<P, T>>> createSubTaskFunction ,
                            Function<D, List<T>> defaultFunction, YmmMapReduceExecutor ymmMapReduceExecutor, Long waitTimeOut, YmmMapReduceMode axxMapReduceMode) {
        if (ymmMapReduceExecutor.enable() && YmmMapReduceMode.MAP_REDUCE == axxMapReduceMode && ymmMapReduceExecutor.getFreeThreads() > 0) {
            this.subTasks = createSubTaskFunction.apply(defaultParams);
            unfinishedTaskCount = new AtomicInteger(subTasks.size());
            if (null != waitTimeOut) {
                this.waitTimeOut = waitTimeOut;
            }
        }
        this.ymmMapReduceExecutor = ymmMapReduceExecutor;
        this.defaultFunction = defaultFunction;
        this.defaultParams = defaultParams;
    }

    /**
     * 任务执行失败回调方法
     * @param ymmMapReduceErrorCode 错误原因
     * @param e 异常信息
     */
    public void callback(YmmMapReduceErrorCode ymmMapReduceErrorCode, Exception e) {
        this.throwException = true;
        ymmMapReduceExecutor.errorLog(ymmMapReduceErrorCode.getMsg(), e);
    }

    /**
     * 任务执行结果
     * @return List</T> 结果集
     */
    public List<T> getTaskResult() {
        if (ymmMapReduceExecutor.enable() && subTasks != null) {
            Integer leftSize = ymmMapReduceExecutor.getFreeThreads();
            if (leftSize >= subTasks.size()) {
                return doMapReduce();
            }
            ymmMapReduceExecutor.warnLog(String.format("queueSize is nearly full. leftSize: %s; taskSize: %s", leftSize, subTasks.size()));
        }
        return doDefaultFunction();
    }

    /**
     *  执行默认函数（非MapReduce模型下的处理逻辑）
     */
    private List<T> doDefaultFunction() {
        long startTime = System.currentTimeMillis();
        List<T> apply = defaultFunction.apply(defaultParams);
        ymmMapReduceExecutor.infoLog(String.format("[uri=%s] [query_str=%s] [time_used=%s]", "Default execute time", defaultParams.getClass(), System.currentTimeMillis() - startTime));
        return apply;
    }

    /**
     *  mapreduce模式执行任务。
     */
    private List<T> doMapReduce() {
        try {
            long startTime = System.currentTimeMillis();
            allTaskFinished.acquire();
            for (YmmMapReduceSubTask<P, T> subTask : subTasks) {
                subTask.initSubTask(this, unfinishedTaskCount, allTaskFinished);
                ymmMapReduceExecutor.submit(subTask);
            }
            if (waitTimeOut > 0) {
                boolean tryAcquire = allTaskFinished.tryAcquire(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!tryAcquire) {
                    callback(YmmMapReduceErrorCode.ERROR_CODE_TIMEOUT, new RuntimeException("TIME OUT"));
                }
            } else {
                allTaskFinished.acquire();
                ymmMapReduceExecutor.infoLog(String.format("[uri=%s] [query_str=%s] [time_used=%s] [msg=%s]", "MapReduce execute time", defaultParams.getClass(), System.currentTimeMillis() - startTime, subTasks.size()));
            }
        } catch (InterruptedException e) {
            callback(YmmMapReduceErrorCode.ERROR_CODE_INTERRUPTED, e);
        } finally {
            allTaskFinished.release();
        }

        List<T> result = new ArrayList<>();

        if (throwException) {
            //  原有逻辑
            return doDefaultFunction();
        } else {
            subTasks.forEach(e->{
                result.addAll(e.getResult());
            });
        }

        return result;
    }

    protected void acquireOneThread() {
        YmmMapReduceExecutor.acquireOneThread();
    }

    protected void releaseOneThread() {
        YmmMapReduceExecutor.releaseOneThread();
    }
}
