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
//        JavaRDD<JSONObject> javaRDD = dataset.toJavaRDD().map(row -> {
//            DaAppPage daAppPage = new DaAppPage();
//            daAppPage.setId(row.getAs("id"));
//            daAppPage.setPage(row.getAs("page"));
//            daAppPage.setPage_name(row.getAs("page_name"));
//            daAppPage.setDescription(row.getAs("description"));
//            daAppPage.setStatus(row.getAs("status"));
//            daAppPage.setCreate_time(row.getAs("create_time"));
//            daAppPage.setCreate_by(row.getAs("create_by"));
//            daAppPage.setUpdate_time(row.getAs("update_time"));
//            daAppPage.setUpdate_by(row.getAs("update_by"));
//            return JSON.parseObject(JSON.toJSONString(daAppPage));
//        });

        JavaRDD<String> javaRDD = dataset.toJavaRDD().map(row -> {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(null != row.getAs("id") ? row.getAs("id").toString() : "").append(",");
            stringBuilder.append(null != row.getAs("page") ? row.getAs("page").toString() : "").append(",");
            stringBuilder.append(null != row.getAs("page_name") ? row.getAs("page_name").toString() : "").append(",");
            stringBuilder.append(null != row.getAs("description") ? row.getAs("description").toString() : "").append(",");
            stringBuilder.append(null != row.getAs("status") ? row.getAs("status").toString() : "").append(",");
            stringBuilder.append(null != row.getAs("create_time") ? row.getAs("create_time").toString() : "").append(",");
            stringBuilder.append(null != row.getAs("create_by") ? row.getAs("create_by").toString() : "").append(",");
            stringBuilder.append(null != row.getAs("update_time") ? row.getAs("update_time").toString() : "").append(",");
            stringBuilder.append(null != row.getAs("update_by") ? row.getAs("update_by").toString() : "");
            return stringBuilder.toString();
        });

        javaRDD.repartition(1).saveAsTextFile("hdfs://n1.dabig.com:9000/ods/bdc_fe_log.log");
    }
}
