//package org.apache.spark.job;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.common.SparkAbstractJob;
//import org.apache.spark.sql.SparkSession;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
//
//import java.io.Serializable;
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * @description
// * @author jiaxiansun
// * @createTime 2021/6/25 15:49
// * @version 1.0
// */
//public class ReadEsToEsJob extends SparkAbstractJob implements Serializable {
//    private static final long serialVersionUID = 1L;
//
//    @Override
//    protected void setConf(SparkConf sparkConf) {
//        sparkConf.set("es.index.auto.create", "true");
//        sparkConf.set("es.nodes", "esn1.dabig.com");
//        sparkConf.set("es.port", "9200");
//        sparkConf.set("es.nodes.wan.only", "false");
//        sparkConf.set("es.net.http.auth.user", "elastic");
//        sparkConf.set("es.net.http.auth.pass", "elastic");
//        //sparkConf.set("es.read.source.filter", "id,kps");
//        sparkConf.set("spark.kryoserializer.buffer.max", "128");
//        sparkConf.set("es.index.read.missing.as.empty", "true");
//        sparkConf.set("es.read.metadata", "true");
//    }
//
//    @Override
//    protected void execute(SparkSession sparkSession, Map<String, String> args) throws Exception {
//        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
//        JavaRDD<String> javaRDD = JavaEsSpark.esJsonRDD(javaSparkContext,"bdc_fe_log", QueryBuilders.termQuery("userName","jiaxiansun").toString()).values();
//        JavaRDD<JSONObject> jsonObjectJavaRDD = javaRDD.map(new Function<String, JSONObject>() {
//            @Override
//            public JSONObject call(String s) throws Exception {
//                JSONObject source = JSON.parseObject(s);
//                String _id = source.getJSONObject("_metadata").getString("_id");
//                source.remove("_metadata");
//                source.put("id",_id);
//                return source;
//            }
//        });
//        Map<String,String> map = new HashMap<>();
//        map.put("es.mapping.id","id");
//        JavaEsSpark.saveToEs(jsonObjectJavaRDD,"bdc_fe_log_spark_test1/_doc", map);
//    }
//}
