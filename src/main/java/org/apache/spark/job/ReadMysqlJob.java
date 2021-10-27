package org.apache.spark.job;

import com.alibaba.fastjson.JSON;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.bean.DaAppPage;
import org.apache.spark.common.SparkAbstractJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @description
 * @author jiaxiansun
 * @createTime 2021/6/25 10:12
 * @version 1.0
 */
public class ReadMysqlJob extends SparkAbstractJob {
    private static final long serialVersionUID = 1L;

    @Override
    protected void setConf(SparkConf conf) {

    }

    @Override
    protected void execute(SparkSession sparkSession, Map<String, String> args) throws Exception {
        Dataset<Row> dataset = readFromMysql(sparkSession, "beacon", "da_app_page");
        JavaRDD<DaAppPage> javaRDD = dataset.toJavaRDD().map(row -> {
            DaAppPage daAppPage = new DaAppPage();
            daAppPage.setId(row.getAs("id"));
            daAppPage.setPage(row.getAs("page"));
            daAppPage.setPage_name(row.getAs("page_name"));
            daAppPage.setDescription(row.getAs("description"));
            daAppPage.setStatus(row.getAs("status"));
            daAppPage.setCreate_time(row.getAs("create_time"));
            daAppPage.setCreate_by(row.getAs("create_by"));
            daAppPage.setUpdate_time(row.getAs("update_time"));
            daAppPage.setUpdate_by(row.getAs("update_by"));
            return daAppPage;
        });
        javaRDD.collect().forEach(s -> {
            System.out.println(JSON.toJSONString(s));
        });
        Dataset<Row> bdcFeLogDataset = sparkSession.createDataFrame(javaRDD, DaAppPage.class);
        save2Mysql(dataset,"big-data","da_app_page", SaveMode.Overwrite);
    }
}
