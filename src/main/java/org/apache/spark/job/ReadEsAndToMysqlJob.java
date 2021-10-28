package org.apache.spark.job;

import com.alibaba.fastjson.JSON;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.bean.BdcFeLog;
import org.apache.spark.common.SparkAbstractJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.io.Serializable;
import java.util.Map;

/**
 * @description
 * @author jiaxiansun
 * @createTime 2021/6/29 17:47
 * @version 1.0
 */
public class ReadEsAndToMysqlJob extends SparkAbstractJob implements Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    protected void setConf(SparkConf sparkConf) {
        sparkConf.set("es.index.auto.create", "true");

        sparkConf.set("es.nodes", "esn1.dabig.com");
        sparkConf.set("es.port", "9200");
        sparkConf.set("es.nodes.wan.only", "false");

        //访问es的用户名
        sparkConf.set("es.net.http.auth.user", "elastic");

        //访问es的密码
        sparkConf.set("es.net.http.auth.pass", "elastic");
        //sparkConf.set("es.read.source.filter", "id,kps");//指定返回字段
        sparkConf.set("spark.kryoserializer.buffer.max", "128");
        sparkConf.set("es.index.read.missing.as.empty", "true");

        //指定metadata返回
        sparkConf.set("es.read.metadata", "true");
    }

    @Override
    protected void execute(SparkSession sparkSession, Map<String, String> args) {
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> javaRDD = JavaEsSpark.esJsonRDD(javaSparkContext,
                "bdc_fe_log",
                QueryBuilders.matchAllQuery().toString())
                .values();
        JavaRDD<BdcFeLog> bdcFeLogJavaRDD = javaRDD.map((Function<String, BdcFeLog>) s -> JSON.parseObject(s, BdcFeLog.class));
        Dataset<Row> bdcFeLogDataset = sparkSession.createDataFrame(bdcFeLogJavaRDD, BdcFeLog.class);
        save2Mysql(bdcFeLogDataset, "beacon", "bdc_fe_log");
    }
}
