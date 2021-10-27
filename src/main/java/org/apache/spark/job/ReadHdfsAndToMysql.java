package org.apache.spark.job;

import com.alibaba.fastjson.JSON;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.bean.DaAppPage;
import org.apache.spark.common.SparkAbstractJob;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Map;

/**
 * @description
 * @author jiaxiansun
 * @createTime 2021/6/30 11:11
 * @version 1.0
 */
public class ReadHdfsAndToMysql extends SparkAbstractJob implements Serializable {

    @Override
    protected void setConf(SparkConf conf) {

    }

    @Override
    protected void execute(SparkSession sparkSession, Map<String, String> args) throws Exception {

        JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile("hdfs://hdn1.dabig.com:8020/ods/mysql/da_app_page.log", 1).toJavaRDD();
        JavaRDD<DaAppPage> daAppPageJavaRDD = javaRDD.map(e -> JSON.parseObject(e, DaAppPage.class));

        Dataset<Row> bdcFeLogDataset = sparkSession.createDataFrame(daAppPageJavaRDD, DaAppPage.class);

        save2Mysql(bdcFeLogDataset, "big-data", "da_app_page");


    }
}
