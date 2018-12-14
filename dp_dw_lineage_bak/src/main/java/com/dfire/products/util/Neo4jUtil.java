package com.dfire.products.util;

import com.dfire.products.dao.Neo4jDaoImpl;

import java.sql.ResultSet;

/**
 * @Description : Neo4j工具类
 * @Author ： HeGuoZi
 * @Date ： 4:39 PM 2018/12/11
 * @Modified :
 */
public class Neo4jUtil {

    private Neo4jDaoImpl neo4jDao = new Neo4jDaoImpl();

    public boolean neo4jCheckTableNodeExist(String databaseName,
                                            String tableName) throws Exception {
        try {
            ResultSet resultSet = neo4jDao.executeQuery("match (n:TableNode{database_name:\"" + databaseName + "\",table_name:\"" + tableName + "\"})\n" +
                    "return count(n)");
            while (resultSet.next()) {
                return resultSet.getInt("count(n)") != 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new Exception("Check neo4j table node error!");
    }

    public boolean neo4jCheckTableRelationExist(String inputDatabaseName,
                                                String inputTableName,
                                                String outputDatabaseName,
                                                String outputTableName) throws Exception {
        try {
            ResultSet resultSet = neo4jDao.executeQuery("match n=(a:TableNode{database_name:\"" + inputDatabaseName
                    + "\",table_name:\"" + inputTableName
                    + "\"})-[r:Flow]->(b:TableNode{database_name:\"" + outputDatabaseName
                    + "\",table_name:\"" + outputTableName + "\"})\n" + "return count(n)");
            while (resultSet.next()) {
                return resultSet.getInt("count(n)") != 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new Exception("Check neo4j table relation error!");
    }


    public boolean neo4jCreateTableNode(String databaseName,
                                        String tableName) throws Exception {
        try {
            return neo4jDao.executeSql(
                    "CREATE (n:TableNode {database_name:\"" + databaseName
                            + "\",table_name:\"" + tableName + "\"});") != 0;
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new Exception("Create neo4j table node error!");
    }

    public boolean neo4jCreateTableRelation(String inputDatabaseName,
                                            String inputTableName,
                                            String outputDatabaseName,
                                            String outputTableName) throws Exception {
        try {
            return neo4jDao.executeSql("MATCH (a:TableNode),(b:TableNode)\n" +
                    "WHERE a.database_name = \"" + inputDatabaseName + "\" \n" +
                    "AND a.table_name = \"" + inputTableName + "\" \n" +
                    "AND b.database_name = \"" + outputDatabaseName + "\" \n" +
                    "AND b.table_name = \"" + outputTableName + "\"\n" +
                    "CREATE (a)-[r:Flow]->(b);") != 0;
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new Exception("Create neo4j table relation error!");
    }


    public boolean neo4jCheckColumnNodeExist(String databaseName,
                                             String tableName,
                                             String columnName) throws Exception {
        try {
            ResultSet resultSet = neo4jDao.executeQuery("match (n:ColumnNode{database_name:\"" + databaseName
                    + "\",table_name:\"" + tableName
                    + "\",column_name:\"" + columnName
                    + "\"})\n" +
                    "return count(n)");
            while (resultSet.next()) {
                return resultSet.getInt("count(n)") != 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new Exception("Check neo4j column node error!");
    }

    public boolean neo4jCheckColumnRelationExist(String inputDatabaseName,
                                                 String inputTableName,
                                                 String inputColumnName,
                                                 String outputDatabaseName,
                                                 String outputTableName,
                                                 String outputColumnName) throws Exception {
        try {
            ResultSet resultSet = neo4jDao.executeQuery("match n=(a:ColumnNode{database_name:\"" + inputDatabaseName
                    + "\",table_name:\"" + inputTableName
                    + "\",column_name:\"" + inputColumnName
                    + "\"})-[r:Flow]->(b:ColumnNode{database_name:\"" + outputDatabaseName
                    + "\",table_name:\"" + outputTableName
                    + "\",column_name:\"" + outputColumnName
                    + "\"})\n" + "return count(n)");
            while (resultSet.next()) {
                return resultSet.getInt("count(n)") != 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new Exception("Check neo4j column relation error!");
    }


    public boolean neo4jCreateColumnNode(String databaseName,
                                         String tableName,
                                         String columnName) throws Exception {
        try {
            return neo4jDao.executeSql(
                    "CREATE (n:ColumnNode {database_name:\"" + databaseName
                            + "\",table_name:\"" + tableName
                            + "\",column_name:\"" + columnName + "\"});") != 0;
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new Exception("Create neo4j column node error!");
    }

    public boolean neo4jCreateColumnRelation(String inputDatabaseName,
                                             String inputTableName,
                                             String inputColumnName,
                                             String outputDatabaseName,
                                             String outputTableName,
                                             String outputColumnName) throws Exception {
        try {
            return neo4jDao.executeSql("MATCH (a:ColumnNode),(b:ColumnNode)\n" +
                    "WHERE a.database_name = \"" + inputDatabaseName + "\" \n" +
                    "AND a.table_name = \"" + inputTableName + "\" \n" +
                    "AND a.column_name = \"" + inputColumnName + "\" \n" +
                    "AND b.database_name = \"" + outputDatabaseName + "\" \n" +
                    "AND b.table_name = \"" + outputTableName + "\"\n" +
                    "AND b.column_name = \"" + outputColumnName + "\"\n" +
                    "CREATE (a)-[r:Flow]->(b);") != 0;
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new Exception("Create neo4j column relation error!");
    }

    public void close() {
        neo4jDao.close();
    }
}
