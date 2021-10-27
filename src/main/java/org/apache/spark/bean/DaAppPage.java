package org.apache.spark.bean;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

/**
 * @description
 * @author jiaxiansun
 * @createTime 2021/6/29 18:14
 * @version 1.0
 */
@Getter
@Setter
public class DaAppPage implements Serializable {
    int id;
    String page;
    String page_name;
    String description;
    int status;
    Timestamp create_time;
    String create_by;
    Timestamp update_time;
    String update_by;

    public DaAppPage() {
        super();
    }

    public DaAppPage(int id, String page, String page_name, String description, int status, Timestamp create_time, String create_by, Timestamp update_time, String update_by) {
        this.id = id;
        this.page = page;
        this.page_name = page_name;
        this.description = description;
        this.status = status;
        this.create_time = create_time;
        this.create_by = create_by;
        this.update_time = update_time;
        this.update_by = update_by;
    }
}
