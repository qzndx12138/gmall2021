package com.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author 刘帅
 * @create 2021-09-27 15:53
 */


public interface AsyncDimJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject jsonObject) throws ParseException;

}