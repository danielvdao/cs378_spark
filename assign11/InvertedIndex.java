package com.refactorlabs.cs378.assign11;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InvertedIndex{
    public static void main(String[] args){
        Utils.printClassPath();

        String inputFilename = args[0];
        String outputFilename = args[1];

        // Create a Java Spark context
        SparkConf conf = new SparkConf().setAppName(InvertedIndex.class.getName()).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the input data
        JavaRDD<String> input = sc.textFile(inputFilename);

        // Split the input into lines
        PairFlatMapFunction<String, String, String> spltFunction = new PairFlatMapFunction<String, String, String>() {

            @Override
            public Iterable<Tuple2<String, String>> call(String verse) throws Exception {
                int[] verse_index = getIndexVerse(verse.toString());
                String id = verse.toString().substring(verse_index[0], verse_index[1]);
                String line[] = stripPunctuation(verse.toString().substring(verse_index[1], verse.toString().length()).toLowerCase());

                Set<String> result = new HashSet<>();
                List<Tuple2<String, String>> verseList= Lists.newArrayList();

                for(String word: line){
                    if(!check_punctuation(word))
                        result.add(word);
                }

                for(String word: result){
                    Tuple2<String, String> resultTuple = new Tuple2<>(word, id);
                    verseList.add(resultTuple);
                }

                return verseList;

            }
        };


        PairFunction<Tuple2<String, String>, String, List<String>> removeIterable = new PairFunction<Tuple2<String, String>, String, List<String>>() {
            @Override
            public Tuple2<String, List<String>> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                List<String> result = Lists.newArrayList();
                result.add(stringStringTuple2._2());
                return new Tuple2<>(stringStringTuple2._1(), result);
            }
        };

        Function2<List<String>, List<String>, List<String>> listFunction = new Function2<List<String>, List<String>, List<String>>() {
            @Override
            public List<String> call(List<String> s, List<String> s2) throws Exception {
                s.addAll(s2);
                Collections.sort(s, new Comparator<String>() {
                    @Override
                    public int compare(String doc_one, String doc_two) {
                        Integer doc_one_chapter = Integer.parseInt(getChapter(doc_one));
                        Integer doc_two_chapter = Integer.parseInt(getChapter(doc_two));

                        Integer doc_one_verse = Integer.parseInt(getVerse(doc_one));
                        Integer doc_two_verse = Integer.parseInt(getVerse(doc_two));

                        int chapter_comparison = doc_one_chapter.compareTo(doc_two_chapter);

                        if (chapter_comparison == 1)
                            return chapter_comparison;
                        else if (chapter_comparison == 0)
                            return doc_one_verse.compareTo(doc_two_verse);
                        else
                            return -1;

                    }
                });
                return s;
            }
        };

        JavaPairRDD<String, String> verses = input.flatMapToPair(spltFunction);
        JavaPairRDD<String, List<String>> verses2 = verses.mapToPair(removeIterable).reduceByKey(listFunction).sortByKey();

        verses2.saveAsTextFile(outputFilename);

        // Shut down context
        sc.stop();

    }


    public static boolean check_punctuation(String word){
        if(word.contains(" ") || word.contains("\n") || word.equals(""))
            return true;
        return false;
    }

    public static String[] stripPunctuation(String dirty){
        return dirty.split("[ \\n\"\\[!?;,.:_()]|[-]{2}");
    }


    public static String getChapter(String doc_id){
        String[] chapter = doc_id.split(":");
        return chapter[1];
    }


    public static String getVerse(String doc_id){
        String[] verse = doc_id.split(":");
        return verse[2];

    }

    public static int[] getIndexVerse(String content){
        String patternString = "\\w+:\\d+:\\d+";
        int[] indices = new int[2];
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(content);

        while(matcher.find()){
            indices[0] = matcher.start();
            indices[1] = matcher.end();
        }

        return indices;
    }


}
