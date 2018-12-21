package com.dfire.entity;

import lombok.Data;

/**
 * @Description :
 * @Author ： HeGuoZi
 * @Date ： 10:37 AM 2018/12/21
 * @Modified :
 */
@Data
public class SearchNeo4jEntity {

    String inputDatabaseName;

    String inputTableName;

    String inputColumnName;

    String outputDatabaseName;

    String outputTableName;

    String outputColumnName;

    String treeDepth;

    String limit;

}
