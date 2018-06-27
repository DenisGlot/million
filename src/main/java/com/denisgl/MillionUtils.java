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
