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

    private static final double[] array = new double[1000_000];
    private static final List<Double> list = new ArrayList<>(1000_000);

    @BeforeClass
    public static void init() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Same number and index");
        sc = new JavaSparkContext(conf);

        for (int i = 0; i < 1000_000; i++) {
            double number = getNumberByAlg(i);
            array[i] = number;
        }

        for (int i = 0; i < 1000_000; i++) {
            list.add(getNumberByAlg(i));
        }

    }

    private static double getNumberByAlg(int number) {
        return (number - 1000) * 2.5;
    }

    @Test
    public void getFirstNumberWithSameIndex_List() {
        double numberWithSameIndex = MillionUtils.getFirstNumberWithSameIndexSpark(list, sc);
        assertEquals(EXPECTED, numberWithSameIndex, 0.0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getNonCrossingNumber_List() {
        List<Double> wrongList = new ArrayList<>(1000_000);
        for (int i = 0; i < 1000_000; i++) {
            wrongList.add(i + 2.0);
        }

        MillionUtils.getFirstNumberWithSameIndexSpark(wrongList, sc);
    }

    @Test
    public void getFirstNumberWithSameIndex_Array() {
        double numberWithSameIndex = MillionUtils.getFirstNumberWithSameIndexSpark(array, sc);
        assertEquals(EXPECTED, numberWithSameIndex, 0.0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getNonCrossingNumber_Array() {
        double[] array = new double[1000_000];
        for (int i = 0; i < 1000_000; i++) {
            double number = i + 2.0;
            array[i] = number;
        }

        MillionUtils.getFirstNumberWithSameIndexSpark(array, sc);
    }
}
