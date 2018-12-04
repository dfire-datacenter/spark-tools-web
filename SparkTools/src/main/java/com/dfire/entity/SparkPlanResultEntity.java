package com.dfire.entity;

import lombok.Data;

/**
 * @Description : Spark解析sql 返回entity
 * @Author ： HeGuoZi
 * @Date ： 16:32 2018/11/15
 * @Modified :
 */
@Data
public class SparkPlanResultEntity {

    boolean isSuccess;

    String inputTables;

    String outputTables;

    String withTables;

    String errorMsg;

    public SparkPlanResultEntity(boolean isSuccess,
                                 String inputTables,
                                 String outputTables,
                                 String withTables,
                                 String errorMsg) {
        this.isSuccess = isSuccess;
        this.inputTables = inputTables;
        this.outputTables = outputTables;
        this.withTables = withTables;
        this.errorMsg = errorMsg;
    }

}
