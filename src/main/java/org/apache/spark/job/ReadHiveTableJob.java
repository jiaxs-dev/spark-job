package org.apache.spark.job;

import groovy.sql.DataSet;
import org.apache.spark.SparkConf;
import org.apache.spark.common.SparkAbstractJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Map;

/**
 * @Description
 * @Author jiaxiansun
 * @date 2021/11/3 16:50
 */
public class ReadHiveTableJob extends SparkAbstractJob implements Serializable {

    @Override
    protected void setConf(SparkConf conf) {

    }

    @Override
    protected void execute(SparkSession sparkSession, Map<String, String> args) throws Exception {
        Dataset<Row> dataSet = sparkSession.sql("select count(*) as cnt from bdc.bdc_fe_log");
        dataSet.show();
    }
}
