package org.apache.spark.common.utils;

import scala.collection.JavaConverters;
import scala.collection.Map$;

import java.util.Map;

/**
 * @description
 * @author jiaxiansun
 * @createTime 2021/6/25 16:36
 * @version 1.0
 */
public class MapUtils {
    public static scala.collection.immutable.Map<String, String> javaMapConvertToScalaMap(Map<String, String> map) {
        scala.collection.mutable.Map<String, String> scalaMap = JavaConverters.mapAsScalaMapConverter(map).asScala();
        Object objMap = Map$.MODULE$.<String, String>newBuilder().$plus$plus$eq(scalaMap.toSeq());
        Object BuildResObjMap = ((scala.collection.mutable.Builder) objMap).result();
        scala.collection.immutable.Map<String, String> targetScalaMap = (scala.collection.immutable.Map) BuildResObjMap;
        return targetScalaMap;
    }
}
