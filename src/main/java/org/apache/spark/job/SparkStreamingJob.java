package org.apache.spark.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.common.SparkAbstractJob;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Map;

/**
 * @description
 * @author jiaxiansun
 * @createTime 2021/6/25 17:13
 * @version 1.0
 */
@Slf4j
public class SparkStreamingJob extends SparkAbstractJob implements Serializable {
    private static final long serialVersionUID = 1L;


    @Override
    protected void setConf(SparkConf conf) {

    }

    @Override
    protected void execute(SparkSession sparkSession, Map<String, String> args) throws Exception {

    }
}
