package com.app.func;

import com.utils.SplitWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author 刘帅
 * @create 2021-10-08 15:19
 */

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction {
    public void eval(String keyWord){
        List<String> words = SplitWordUtil.splitKeyWord(keyWord);

        for (String word : words){
            collect(Row.of(word));
        }
    }
}
