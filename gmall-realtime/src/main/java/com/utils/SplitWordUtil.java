package com.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 刘帅
 * @create 2021-10-08 15:22
 */


public class SplitWordUtil {
    public static List<String> splitKeyWord(String keyWord){
        ArrayList<String> words = new ArrayList<>();

        StringReader reader = new StringReader(keyWord);

        //构建分词对象
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        try {
            //获取下一个分词
            Lexeme next = ikSegmenter.next();

            while (next != null){
                String word = next.getLexemeText();  //获取分词对象中的数据，添加到ArrayList中
                words.add(word);

                next = ikSegmenter.next();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return words;
    }

    public static void main(String[] args) {
        System.out.println(splitKeyWord("LNG牛逼，ale贾克斯无敌，恭喜LNG晋级英雄联盟S11世界赛小组赛"));
    }
}
