package org.apache.spark.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.common.config.CustomProperties;
import org.apache.spark.common.jdbc.JdbcConsts;
import org.apache.spark.sql.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import java.util.Map;
import java.util.Properties;

/**
 * @Title: SparkAbstractJob
 * @Package: com.dongao.common
 * @Author: jiaxiansun@dongao.com
 * @Date: 2020/4/3
 * @Time: 14:47
 * @Description: todo
 * @Copyright: www.dongao.com@2020
 */
@Slf4j
public abstract class SparkAbstractJob {

    /**
     * spark配置项
     *
     * @param conf
     */
    protected abstract void setConf(SparkConf conf);

    /**
     * 任务执行体
     *
     * @param sparkSession SparkSession实例
     * @param args         入参集合
     * @throws Exception
     */
    protected abstract void execute(SparkSession sparkSession, Map<String, String> args) throws Exception;

    /**
     * 初始化任务并执行
     *
     * @param args 入参
     */
    public final void execute(Map<String, String> args) {
        log.info("Running " + this.getClass().getName() + " ，args：" + args);
        SparkSession sparkSession = null;
        try {
            final SparkConf sparkConf = initSparkConf(args.get(JobConsts.ARGS_CLASS_NAME));
            // 是否本地执行
            final String master = args.getOrDefault("_master", "yarn");
            if ("local".equalsIgnoreCase(master)) {
                sparkConf.setMaster("local[*]");
            }
            // 支持个性化conf设置
            setConf(sparkConf);
            sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
            // 执行具体任务
            execute(sparkSession, args);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            throw new RuntimeException("error execute " + this.getClass().getName(), e);
        } finally {
            if (sparkSession != null) {
                sparkSession.stop();
            }
        }
    }

    /**
     * 初始化spark conf
     *
     * @param appName 应用名称
     * @return
     */
    private SparkConf initSparkConf(String appName) {
        return new SparkConf().setAppName(appName)
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                // 压缩
                .set("hive.exec.compress.output", "true")
                .set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec")
                // 动态分区
                .set("hive.exec.dynamic.partition", "true")
                .set("hive.exec.dynamic.partition.mode", "nonstrict")
                // 开启小文件合并
                .set("spark.sql.adaptive.enabled", "true")
                //在 map only 的任务结束时合并小文件
                .set("hive.merge.mapfiles", "true")
                // true 时在 MapReduce 的任务结束时合并小文件
                .set("hive.merge.mapredfiles", "true")
                //合并文件的大小
                .set("hive.merge.size.per.task", "256000000")
                //每个 Map 最大分割大小
                .set("mapred.max.split.size", "256000000")
                // 平均文件大小，是决定是否执行合并操作的阈值，默认16000000
                .set("hive.merge.smallfiles.avgsize", "256000000")
                // 执行 Map 前进行小文件合并
                .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
                // 大小写不敏感
                .set("spark.sql.caseSensitive", "false")
                //限制spark输出往hdfs写success文件
                .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
    }

    /**
     * 保存数据到Mysql(默认是追加模式，如需覆盖调用带mode的方法)
     *
     * @param df        数据集
     * @param dbName    数据库名
     * @param tableName 数据表名
     */
    protected void save2Mysql(Dataset<Row> df, String dbName, String tableName) {
        this.save2Mysql(df, dbName, tableName, SaveMode.Append);
    }

    /**
     * 保存数据到Mysql，注意Overwrite模式，我们不删除表，只是清除数据，因为删除表存在权限问题
     *
     * @param df        数据集
     * @param dbName    数据库名
     * @param tableName 数据表名
     * @param mode      保存模式
     */
    protected void save2Mysql(Dataset<Row> df, String dbName, String tableName, SaveMode mode) {
        if (null == mode) {
            mode = SaveMode.Append; // 默认追加模式
        }
        final DataFrameWriter<Row> option = df.write()
                .format("jdbc")
                .mode(mode)
                .option("driver", CustomProperties.JDBC.getProperty(JdbcConsts.getDriverKey(JdbcConsts.DB_TYPE_MYSQL, dbName)))
                .option("url", CustomProperties.JDBC.getProperty(JdbcConsts.getUrlKey(JdbcConsts.DB_TYPE_MYSQL, dbName)))
                .option("dbtable", tableName)
                .option("user", CustomProperties.JDBC.getProperty(JdbcConsts.getUserKey(JdbcConsts.DB_TYPE_MYSQL, dbName)))
                .option("password", CustomProperties.JDBC.getProperty(JdbcConsts.getPasswdKey(JdbcConsts.DB_TYPE_MYSQL, dbName)))
                .option("numPartitions", 4)
                .option("batchsize", 5000) // default 1000
                // 写mysql优化：配置numPartitions、batchsize，最关键的是url中配置rewriteBatchedStatements=true，即打开mysql的批处理能力
                ;

        log.info(dbName + "." + tableName + " 写入模式：" + mode);
        // 重写模式下，采用清空表的方式 ，注意，必须有drop的权限才可以，否则会一直报错没权限
        if (mode == SaveMode.Overwrite) {
            option.option("truncate", true).save();
        } else {
            option.save();
        }

    }

    /**
     * 保存数据到ClickHouse，注意Overwrite模式，我们不删除表，只是清除数据，因为删除表存在权限问题
     *
     * @param df        数据集
     * @param dbName    数据库名
     * @param tableName 数据表名
     * @param mode      保存模式
     */
    protected void save2Clickhouse(Dataset<Row> df, String dbName, String tableName, SaveMode mode) {
        if(null == mode){
            mode = SaveMode.Append; // 默认追加模式
        }
        final DataFrameWriter<Row> option = df.write()
                .format("jdbc")
                .mode(mode)
                .option("driver", CustomProperties.JDBC.getProperty(JdbcConsts.getDriverKey(JdbcConsts.DB_TYPE_CLICKHOUSE, dbName)))
                .option("url", CustomProperties.JDBC.getProperty(JdbcConsts.getUrlKey(JdbcConsts.DB_TYPE_CLICKHOUSE, dbName)))
                .option("dbtable", tableName)
                .option("user", CustomProperties.JDBC.getProperty(JdbcConsts.getUserKey(JdbcConsts.DB_TYPE_CLICKHOUSE, dbName)))
                .option("password", CustomProperties.JDBC.getProperty(JdbcConsts.getPasswdKey(JdbcConsts.DB_TYPE_CLICKHOUSE, dbName)))
                .option("numPartitions", 4)
                .option("batchsize",200000) // default 1000
                // 写mysql优化：配置numPartitions、batchsize，最关键的是url中配置rewriteBatchedStatements=true，即打开mysql的批处理能力
                .option("isolationLevel", "NONE")
                ;

        log.info(dbName+"."+tableName+" 写入模式：" + mode);
        // 重写模式下，采用清空表的方式 ，注意，必须有drop的权限才可以，否则会一直报错没权限
        if(mode == SaveMode.Overwrite) {
            option.option("truncate", true).save();
        }else {
            option.save();
        }

    }

    /**
     * 从mysql库中读取指定库.表数据（注意是全表数据！）
     *
     * @param sparkSession
     * @param dbName       数据库名
     * @param tableName    数据表名
     */
    protected Dataset<Row> readFromMysql(SparkSession sparkSession, String dbName, String tableName) {
        String url = CustomProperties.JDBC.getProperty(JdbcConsts.getUrlKey(JdbcConsts.DB_TYPE_MYSQL, dbName));
        //增加数据库的用户名(user)密码(password),指定数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", CustomProperties.JDBC.getProperty(JdbcConsts.getUserKey(JdbcConsts.DB_TYPE_MYSQL, dbName)));
        connectionProperties.put("password", CustomProperties.JDBC.getProperty(JdbcConsts.getPasswdKey(JdbcConsts.DB_TYPE_MYSQL, dbName)));
        connectionProperties.put("driver", CustomProperties.JDBC.getProperty(JdbcConsts.getDriverKey(JdbcConsts.DB_TYPE_MYSQL, dbName)));
        return sparkSession.read().jdbc(url, tableName, connectionProperties).select("*");
    }

    /**
     * 保存数据到Phoenix
     *
     * @param df        数据集
     * @param tableName 数据表名
     */
    protected void save2Phoenix(Dataset<Row> df, String tableName) {
        df.write()
                .format("org.apache.phoenix.spark")
                .mode("overwrite")
                .option("table", tableName)
                .option("zkUrl", CustomProperties.CONFIG.getProperty(DABIG_ZK_URL))
                .save();
    }

    /**
     * 将指定字符串时间转换为 org.joda.time.DateTime 类型
     *
     * @param targetDt  需要转换的目标字符串时间，如："20200501"
     * @param formatter 需要转换的目标字符串时间格式，如："yyyyMMdd"
     * @return
     */
    protected static DateTime parseDate(String targetDt, String formatter) {
        return DateTimeFormat.forPattern(formatter)
                // 不指定时区，特殊日期如：19880410会抛异常
                .withZone(DateTimeZone.forOffsetHours(8))
                .parseDateTime(targetDt);
    }

    /**
     * 大数据环境zk URL
     */
    public static final String DABIG_ZK_URL = "dabig.zk.url";
    /**
     * etl consumer kafka 消费主题
     */
    public static final String ETL_CONSUMER_KAFKA_TOPIC = "etl.consumer.kafka.topic";
    /**
     * etl consumer kafka 消费主组
     */
    public static final String ETL_CONSUMER_KAFKA_GROUP = "etl.consumer.kafka.group";

}
