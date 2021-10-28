package org.apache.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.common.JobConsts;
import org.apache.spark.common.SparkAbstractJob;
import org.apache.spark.common.config.CustomProperties;
import org.apache.spark.common.jdbc.JdbcConsts;

import java.util.HashMap;
import java.util.Map;

/**
 * @description spark入口
 * @author jiaxiansun
 * @createTime 2021/9/10 15:08
 * @version 1.0
 */
@Slf4j
public class SparkEntrance {

    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        String className = "org.apache.spark.job.ReadEsAndToMysqlJob";
        final Object o = Class.forName(className).newInstance();
        SparkAbstractJob abstractApplication = (SparkAbstractJob) o;
        Map<String, String> params = new HashMap<>(1);
        params.put(JobConsts.ARGS_CLASS_NAME, "ReadEsAndToMysqlJob");

        abstractApplication.execute(params);
    }

//    public static void main(String[] args) throws Exception {
//        long start = System.currentTimeMillis();
//        log.info("SparkEntry args:" + StringUtils.join(args, " "));
//        final Map<String, String> params = new HashMap<>();
//        try {
//
//
//            if (args.length < 1) {
//                throw new IllegalArgumentException("className is required");
//            }
//            // 格式化参数集
//            initParams(params, args);
//            if (StringUtils.isBlank(params.get(JobConsts.ARGS_CLASS_NAME))) {
//                throw new IllegalArgumentException("className is required");
//            }
//            final String className = params.get(JobConsts.ARGS_CLASS_NAME);
//            final Object o = Class.forName(className).newInstance();
//            if (!(o instanceof SparkAbstractJob)) {
//                throw new IllegalArgumentException(className + " is not a subClass of SparkAbstractJob");
//            }
//            SparkAbstractJob abstractApplication = (SparkAbstractJob) o;
//            abstractApplication.execute(params);
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            long end = System.currentTimeMillis();
//            log.info("this task cost {} ms",end-start);
//        }
//    }

    private static void initParams(Map<String, String> params, String[] args) {
        if (args != null && args.length >= 1) {
            if (StringUtils.isNotBlank(args[0])) {
                final String[] execParams = args[0].split(",");
                for (String execParam : execParams) {
                    if (StringUtils.isNotBlank(execParam)) {
                        final String[] split = execParam.split("=");
                        if (split.length == 2 && StringUtils.isNotBlank(split[0])) {
                            params.put(split[0], split[1]);
                        }
                    }
                }
            }
        }
        log.info("format parameters：" + params);
    }

    /**
     * 计算时间计量单位
     *
     * @param millis 毫秒
     * @return
     */
    public static String convertTimeUnit(Long millis) {
        if (millis == null) {
            return "0s";
        }
        // 使用小时
        if (millis > (1000 * 60 * 60)) {
            return (millis / (1000 * 60 * 60)) + "h " + ((millis % (1000 * 60 * 60)) / (1000 * 60)) + "m";
        }
        // 使用分钟
        if (millis > (1000 * 60)) {
            return (millis / (1000 * 60)) + "m " + ((millis % (1000 * 60)) / 1000) + "s";
        }
        // 使用秒
        if (millis > 1000) {
            return (millis / 1000) + "s " + (millis % 1000) + "ms";
        }
        // 使用毫秒
        return millis + "ms";
    }
}
