package org.apache.spark.bean;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @description
 * @author jiaxiansun
 * @createTime 2021/6/25 15:53
 * @version 1.0
 */
@Setter
@Getter
public class BdcFeLog implements Serializable {

    String browserType;
    String city;
    long countTime;
    String eventType;
    String ip;
    String loginType;
    String msgCode;
    String os;
    String phone;
    String recordTime;
    String referrerName;
    String referrerPath;
    String requestMethod;
    String requestParams;
    String requestUrl;
    long responseCode;
    String responseMsg;
    String routerName;
    String routerPath;
    long status;
    String userAgent;
    String userName;
    String userTrueName;

    public BdcFeLog() {
        super();
    }

    public BdcFeLog(String browserType, String city, long countTime, String eventType, String ip, String loginType, String msgCode, String os, String phone, String recordTime, String referrerName, String referrerPath, String requestMethod, String requestParams, String requestUrl, long responseCode, String responseMsg, String routerName, String routerPath, long status, String userAgent, String userName, String userTrueName) {
        this.browserType = browserType;
        this.city = city;
        this.countTime = countTime;
        this.eventType = eventType;
        this.ip = ip;
        this.loginType = loginType;
        this.msgCode = msgCode;
        this.os = os;
        this.phone = phone;
        this.recordTime = recordTime;
        this.referrerName = referrerName;
        this.referrerPath = referrerPath;
        this.requestMethod = requestMethod;
        this.requestParams = requestParams;
        this.requestUrl = requestUrl;
        this.responseCode = responseCode;
        this.responseMsg = responseMsg;
        this.routerName = routerName;
        this.routerPath = routerPath;
        this.status = status;
        this.userAgent = userAgent;
        this.userName = userName;
        this.userTrueName = userTrueName;
    }
}
