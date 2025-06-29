package com.atguigu.gmall.realtime.app.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    public static List<String> splitKeyword(String keyword) throws IOException {

        //创建集合用于存放切分后的单词
        ArrayList<String> wordList = new ArrayList<>();

        //创建分词器对象 true:ik_smart  false:ik_max_word
        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(keyword), false);

        //获取切分后的词
        Lexeme lexeme = ikSegmenter.next();

        while (lexeme != null) {
            String word = lexeme.getLexemeText();
            wordList.add(word);

            lexeme = ikSegmenter.next();
        }

        //返回结果
        return wordList;
    }

    public static void main(String[] args) throws IOException {

        System.out.println(splitKeyword("程序员"));

    }

}
