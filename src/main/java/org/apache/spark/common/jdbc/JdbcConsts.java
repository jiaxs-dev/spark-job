package org.apache.spark.common.jdbc;

public class JdbcConsts {

    public static final String DB_NAME_BEACON = "beacon";
    // -------------------------- db type ---------------------------------
    /**
     * 数据库类型：mysql
     */
    public static final String DB_TYPE_CLICKHOUSE = "clickhouse";
    /**
     * 数据库类型：mysql
     */
    public static final String DB_TYPE_MYSQL = "mysql";
    /**
     * 数据库类型：hive
     */
    public static final String DB_TYPE_HIVE = "hive";
    /**
     * 数据库类型：phoenix
     */
    public static final String DB_TYPE_PHOENIX = "phoenix";

    // -------------------------- data source ---------------------------------
    /**
     * 数据源参数：driverClassName
     */
    public static final String DS_DRIVERCLASSNAME = "driverClassName";
    /**
     * 数据源参数：url
     */
    public static final String DS_URL = "url";
    /**
     * 数据源参数：username
     */
    public static final String DS_USERNAME = "username";
    /**
     * 数据源参数：password
     */
    public static final String DS_PASSWORD = "password";

    /**
     * 获取指定数据库的驱动key
     *
     * @param dbType 数据库类型
     * @param dbName 数据库名
     * @return
     */
    public static String getDriverKey(String dbType, String dbName) {
        return "jdbc." + dbType + "." + dbName + "." + DS_DRIVERCLASSNAME;
    }

    /**
     * 获取指定数据库的url key
     *
     * @param dbType 数据库类型
     * @param dbName 数据库名
     * @return
     */
    public static String getUrlKey(String dbType, String dbName) {
        return "jdbc." + dbType + "." + dbName + "." + DS_URL;
    }

    /**
     * 获取指定数据库的用户名key
     *
     * @param dbType 数据库类型
     * @param dbName 数据库名
     * @return
     */
    public static String getUserKey(String dbType, String dbName) {
        return "jdbc." + dbType + "." + dbName + "." + DS_USERNAME;
    }

    /**
     * 获取指定数据库的密码key
     *
     * @param dbType 数据库类型
     * @param dbName 数据库名
     * @return
     */
    public static String getPasswdKey(String dbType, String dbName) {
        return "jdbc." + dbType + "." + dbName + "." + DS_PASSWORD;
    }
}
