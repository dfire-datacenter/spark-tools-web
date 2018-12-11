package com.dfire.products.util;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DBUtil {

    public enum DB_TYPE {
        META, TASK
    }

    private String     driver;
    private String     url;
    private String     user;
    private String     password;
    private Connection mysqlConn;

    private String     sparkDriver;
    private String     sparkUrl;
    private String     sparkUser;
    private String     sparkPassword;
    private Connection sparkConn;

    public DBUtil() {

        this.sparkDriver = "org.apache.hive.jdbc.HiveDriver";
        this.sparkUrl = "jdbc:hive2://10.10.18.215:10000";
        this.sparkUser = "import";
        this.sparkPassword = "import";

        this.driver = "com.mysql.jdbc.Driver";
        this.url = "jdbc:mysql://10.1.6.10:3306";
        this.user = "twodfire";
        this.password = "123456";

//        this.driver = PropertyFileUtil.getProperty(type.name().toLowerCase() + ".jdbc.driverClassName");
//        this.url = PropertyFileUtil.getProperty(type.name().toLowerCase() + ".jdbc.url");
//        this.user = PropertyFileUtil.getProperty(type.name().toLowerCase() + ".jdbc.username");
//        this.password = PropertyFileUtil.getProperty(type.name().toLowerCase() + ".jdbc.password");
    }

    private void setMysqlConn() {
        if (mysqlConn == null) {
            try {
                Class.forName(driver);
                this.mysqlConn = DriverManager.getConnection(url, user, password);
            } catch (ClassNotFoundException classnotfoundexception) {
                classnotfoundexception.printStackTrace();
                System.err.println("db: " + classnotfoundexception.getMessage());
            } catch (SQLException sqlexception) {
                System.err.println("db.getconn(): " + sqlexception.getMessage());
            }
        }
    }

    private void setSparkConn() {
        if (sparkConn == null) {
            try {
                Class.forName(sparkDriver);
                this.sparkConn = DriverManager.getConnection(sparkUrl, sparkUser, sparkPassword);
            } catch (ClassNotFoundException classnotfoundexception) {
                classnotfoundexception.printStackTrace();
                System.err.println("db: " + classnotfoundexception.getMessage());
            } catch (SQLException sqlexception) {
                System.err.println("db.getconn(): " + sqlexception.getMessage());
            }
        }
    }

    public int doInsert(String sql) throws Exception {
        return doUpdate(sql);
    }

    public int doDelete(String sql) throws Exception {
        return doUpdate(sql);
    }

    public int doUpdate(String sql) throws Exception {
        Statement stmt = null;
        try {
            setMysqlConn();
            stmt = mysqlConn.createStatement();
            return stmt.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
//        finally {
//            close(null, stmt);
//        }
    }

    public List<Map<String, Object>> doSelect(String sql) throws Exception {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            setMysqlConn();
            stmt = mysqlConn.createStatement(
                    java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery(sql);
            List<Map<String, Object>> list = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> map = rowToMap(rs, rs.getRow());
                Map<String,Integer> tableInfo = new HashMap<>();
                list.add(map);
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
//        finally {
//            close(rs, stmt);
//        }
    }


    private Map<String, Object> rowToMap(ResultSet resultset, int rowNum) throws SQLException {
        ResultSetMetaData rsmd = resultset.getMetaData();
        int columnNum = rsmd.getColumnCount();
        Map<String, Object> map = new HashMap<>(columnNum + 5);
        for (int i = 1; i <= columnNum; i++) {
            String columnName = rsmd.getColumnLabel(i);
            map.put(columnName, resultset.getObject(columnName));
        }
        return map;
    }

    /**
     * �ر���ݿ�����ݿ����������ݿ�����
     *
     * @Function: Close all the statement and conn int this instance and close
     * the parameter ResultSet
     * @Param: ResultSet
     * @Exception: SQLException, Exception
     **/
    public void close(ResultSet rs, Statement stmt) throws Exception {
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (mysqlConn != null) {
            mysqlConn.close();
        }
    }

    public void initLineageTable(int tableId, String table, String database) throws Exception {
        doInsert("insert into data_lineage.data_lineage_table " +
                "(`table_id`,`table_name`,`database_name`) " +
                "values('" + tableId + "','" + table + "','" + database + "')");
    }

    public void initLineageColumn(int tableId, String tableName, int columnId, String columnName) throws Exception {
        doInsert("insert into data_lineage.data_lineage_column " +
                "(`column_id`,`column_name`,`table_id`,`table_name`) " +
                "values('" + columnId + "','" + columnName + "','" + tableId + "','" + tableName + "')");
    }

    public static void main(String[] args) {
        try {
            DBUtil db = new DBUtil();
            List<Map<String, Object>> rs = db.doSelect("select * from heguozi.dtl_t_cm_od_tp_in_join limit 5");
            for (Map<String, Object> map : rs) {
                for (Entry<String, Object> entry : map.entrySet()) {
                    System.out.println(entry.getKey() + ":" + entry.getValue() + ",");
                }
                System.out.println("");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
