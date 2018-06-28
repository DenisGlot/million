package com.denisgl;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class MillionUtils {

    public static double getNumberWithSameIndex(List<Double> list) {
        return getNumberWithSameIndex(list, 0, list.size() - 1);
    }

    private static double getNumberWithSameIndex(List<Double> list, int start, int end) {
        int half = (start + end) / 2;
        Double element = list.get(half);

        if (half == element.intValue()) {
            return element;
        } else if (half < element.intValue()) {
            return getNumberWithSameIndex(list, start, --half);
        } else {
            return getNumberWithSameIndex(list, ++half, end);
        }
    }

    public static double getNumberWithSameIndex(double[] array) {
        return getNumberWithSameIndex(array, 0, array.length - 1);
    }

    private static double getNumberWithSameIndex(double[] array, int start, int end) {
        int half = (start + end) / 2;

        if (half == (int) array[half]) {
            return array[half];
        } else if (array[half] > half) {
            return getNumberWithSameIndex(array, start, half - 1);
        } else {
            return getNumberWithSameIndex(array, half + 1, end);
        }
    }


    //Search with spark


    public static double getFirstNumberWithSameIndex(double[] array) {
        return getFirstNumberWithSameIndexSpark(array, null);
    }

    public static double getFirstNumberWithSameIndexSpark(double[] array, JavaSparkContext sc) {
        Double[] doubles = ArrayUtils.toObject(array);
        List<Double> doubleList = Arrays.asList(doubles);
        return getFirstNumberWithSameIndexSpark(doubleList, sc);
    }

    public static double getFirstNumberWithSameIndex(List<Double> list) {
        return getFirstNumberWithSameIndexSpark(list, null);
    }

    public static double getFirstNumberWithSameIndexSpark(List<Double> list, JavaSparkContext sc) {
        if (sc == null) {
            SparkConf conf = new SparkConf().setMaster("local").setAppName("Same number and index");
            sc = new JavaSparkContext(conf);
        }

        JavaRDD<Double> arrayRDD = sc.parallelize(list);
        JavaPairRDD<Double, Long> arrayRDDIndex = arrayRDD.zipWithIndex();
        Tuple2<Double, Long> firstTruple = arrayRDDIndex
                .filter((n) -> n._1.longValue() == n._2)
                .first();
        return firstTruple._1();
    }

}
