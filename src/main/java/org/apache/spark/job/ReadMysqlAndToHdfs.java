package org.apache.spark.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.common.SparkAbstractJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Map;

/**
 * @description
 * @author jiaxiansun
 * @createTime 2021/6/29 18:36
 * @version 1.0
 */
public class ReadMysqlAndToHdfs extends SparkAbstractJob implements Serializable {

    @Override
    protected void setConf(SparkConf conf) {

    }

    @Override
    protected void execute(SparkSession sparkSession, Map<String, String> args) throws Exception {
        Dataset<Row> dataset = readFromMysql(sparkSession, "beacon", "bdc_fe_log");
        dataset.registerTempTable("tmp_bdc_fe_log");
        dataset.write().parquet("hdfs://hdn1.dabig.com:9000/user/hive/warehouse/tmp_bdc_fe_log");
//        JavaRDD<String> javaRDD = dataset.toJavaRDD().map(row -> {
//            StringBuilder stringBuilder = new StringBuilder();
//            stringBuilder.append(null != row.getAs("userName") ? row.getAs("userName").toString() : "").append(",");
//            stringBuilder.append(null != row.getAs("browserType") ? row.getAs("browserType").toString() : "").append(",");
//            stringBuilder.append(null != row.getAs("recordTime") ? row.getAs("recordTime").toString() : "");
//            return stringBuilder.toString();
//        });
//
////        javaRDD.saveAsTextFile("hdfs://hdn1.dabig.com:9000/ods/bdc_fe_log.log");
//        javaRDD.saveAsTextFile("hdfs://hdn1.dabig.com:9000/ods/bdc_fe_log_p.log");
    }
}
