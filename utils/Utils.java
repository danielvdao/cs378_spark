package com.refactorlabs.cs378.utils;


import java.net.URL;
import java.net.URLClassLoader;

public class Utils {
    public static final String REDUCER_COUNTER_GROUP = "Reducer Counts";
    public static final String MAPPER_COUNTER_GROUP = "Mapper Counts";
    public static final long ONE = 1L;

    public static void printClassPath() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader) cl).getURLs();
        System.out.println("classpath BEGIN");
        for (URL url : urls) {
            System.out.println(url.getFile());
        }
        System.out.println("classpath END");
    }
}
