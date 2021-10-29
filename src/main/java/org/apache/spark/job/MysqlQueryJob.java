package org.apache.spark.job;

import org.apache.spark.SparkConf;
import org.apache.spark.common.SparkAbstractJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * @Description
 * @Author jiaxiansun
 * @date 2021/10/29 9:54
 */
public class MysqlQueryJob extends SparkAbstractJob implements Serializable {

    @Override
    protected void setConf(SparkConf conf) {

    }

    @Override
    protected void execute(SparkSession sparkSession, Map<String, String> args) throws Exception {
        Properties mysqlConnectionProperties = mysqlConnectionProperties("beacon");
        Dataset<Row> dataset = sparkSession.read()
                .jdbc(mysqlConnectionProperties.getProperty("url"), "bdc_fe_log", mysqlConnectionProperties)
                .select("userName", "browserType", "recordTime");
        dataset.createTempView("bdc_fe_log");
        Dataset<Row> result = sparkSession.sql("select count(*) as cnt,count(distinct userName) as mem , browserType,substring(recordTime,1,10) as d from bdc_fe_log group by browserType,substring(recordTime,1,10) order by browserType,substring(recordTime,1,10)");
//        save2Mysql(result, "beacon", "bdc_statis");
        result.createOrReplaceTempView("tmpStatis");
        sparkSession.sql("select * from tmpStatis").show();

        sparkSession.sql("insert into table bdc.bdc_statis select * from tmpStatis");
    }
}
