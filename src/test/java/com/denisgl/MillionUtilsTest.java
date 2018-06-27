package com.denisgl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MillionUtilsTest {

    //1667 cross number
    private static final double EXPECTED = 1667.5;

    private static JavaSparkContext sc;

    @BeforeClass
    public static void initSpark() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Same number and index");
        sc = new JavaSparkContext(conf);
    }

    @Test
    public void getFirstNumberWithSameIndexList() {
        List<Double> list = new ArrayList<>();
        for (int i = 0; i < 1000_000; i++) {
            list.add(getNumberByAlg(i));
        }
        double numberWithSameIndex = MillionUtils.getFirstNumberWithSameIndexSpark(list, sc);
        assertEquals(EXPECTED, numberWithSameIndex, 0.0);
    }

    @Test
    public void getFirstNumberWithSameIndexArray() {
        double[] array = new double[1000_000];

        for (int i = 0; i < 1000_000; i++) {
            double number = getNumberByAlg(i);
            array[i] = number;
        }

        double numberWithSameIndex = MillionUtils.getFirstNumberWithSameIndexSpark(array, sc);
        assertEquals(EXPECTED, numberWithSameIndex, 0.0);
    }

    private double getNumberByAlg(int number) {
        return (number - 1000) * 2.5;
    }
}
