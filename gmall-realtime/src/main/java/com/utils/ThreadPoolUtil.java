package com.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author 刘帅
 * @create 2021-09-27 15:51
 */


public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {

        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(4,
                            20,
                            60,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>());
                }
            }
        }

        return threadPoolExecutor;
    }
}
