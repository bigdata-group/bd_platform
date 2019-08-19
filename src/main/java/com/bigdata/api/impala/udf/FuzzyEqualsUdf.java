package com.bigdata.api.impala.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;

import java.util.ArrayList;
import java.util.List;

/**
 * Udf that returns true if two double arguments  are approximately equal.
 * Usage: > drop function fuzzy_equals(double, double);
 *        > create function fuzzy_equals(double, double) returns boolean location '/dataext/impala-code-1.0-SNAPSHOT.jar' SYMBOL='udf.FuzzyEqualsUdf';
 *        > select fuzzy_equals(1, 1.000001);
 */
public class FuzzyEqualsUdf extends UDF {
    public FuzzyEqualsUdf() {
    }

    public BooleanWritable evaluate(DoubleWritable x, DoubleWritable y) {
        List<String> lst = new ArrayList<String>();
        double EPSILON = 0.000001f;
        if (x == null || y == null) return null;
        return new BooleanWritable(Math.abs(x.get() - y.get()) < EPSILON);
    }
}
