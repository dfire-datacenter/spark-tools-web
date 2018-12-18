package com.dfire.products.util;

import com.dfire.products.util.pool.MysqlConnectionPool;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DBUtil {

    private String lineageMysqlDriver;
    private String lineageMysqlUrl;
    private String lineageMysqlUser;
    private String lineageMysqlPassword;

    private String heraMysqlDriver;
    private String heraMysqlUrl;
    private String heraMysqlUser;
    private String heraMysqlPassword;

    private String     sparkDriver;
    private String     sparkUrl;
    private String     sparkUser;
    private String     sparkPassword;
    private Connection sparkConn;

    /**
     * data_lineage
     */
    private MysqlConnectionPool mysqlConnectionPool;

    /**
     * heraDb
     */
    private MysqlConnectionPool heraConnectionPool;


    public DBUtil() {
        if (!PropertyFileUtil.isLoaded()) {
            PropertyFileUtil.init();
        }
        this.sparkDriver = PropertyFileUtil.getProperty("spark.driver");
        this.sparkUrl = PropertyFileUtil.getProperty("spark.url");
        this.sparkUser = PropertyFileUtil.getProperty("spark.user");
        this.sparkPassword = PropertyFileUtil.getProperty("spark.password");

        this.lineageMysqlDriver = PropertyFileUtil.getProperty("lineage.mysql.driver");
        this.lineageMysqlUrl = PropertyFileUtil.getProperty("lineage.mysql.url");
        this.lineageMysqlUser = PropertyFileUtil.getProperty("lineage.mysql.user");
        this.lineageMysqlPassword = PropertyFileUtil.getProperty("lineage.mysql.password");

        this.heraMysqlDriver = PropertyFileUtil.getProperty("hera.mysql.driver");
        this.heraMysqlUrl = PropertyFileUtil.getProperty("hera.mysql.url");
        this.heraMysqlUser = PropertyFileUtil.getProperty("hera.mysql.user");
        this.heraMysqlPassword = PropertyFileUtil.getProperty("hera.mysql.password");

    }

    private void initHeraConnection() {
        try {
            if (heraConnectionPool == null) {
                heraConnectionPool = new MysqlConnectionPool(this.heraMysqlDriver,
                        this.heraMysqlUrl,
                        this.heraMysqlUser,
                        this.heraMysqlPassword);
                heraConnectionPool.createPool();
            }
        } catch (ClassNotFoundException classnotfoundexception) {
            classnotfoundexception.printStackTrace();
            System.err.println("db: " + classnotfoundexception.getMessage());
        } catch (SQLException sqlexception) {
            System.err.println("db.getconn(): " + sqlexception.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initMysqlConnection() {
        if (mysqlConnectionPool == null) {
            try {
                mysqlConnectionPool = new MysqlConnectionPool(lineageMysqlDriver,
                        lineageMysqlUrl,
                        lineageMysqlUser,
                        lineageMysqlPassword);
                mysqlConnectionPool.createPool();
            } catch (Exception e) {
                e.printStackTrace();
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
        try {
            initMysqlConnection();
            Connection connection = mysqlConnectionPool.getConnection();
            Statement stmt = connection.createStatement();
            int res = stmt.executeUpdate(sql);
            mysqlConnectionPool.returnConnection(connection);
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
//        finally {
//            close(null, stmt);
//        }
    }

    public List<Map<String, Object>> doSelect(String sql) throws Exception {
        try {
            initMysqlConnection();
            Connection connection = mysqlConnectionPool.getConnection();
            Statement stmt = connection.createStatement(
                    java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = stmt.executeQuery(sql);
            List<Map<String, Object>> list = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> map = rowToMap(rs, rs.getRow());
                list.add(map);
            }
            mysqlConnectionPool.returnConnection(connection);
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public List<Map<String, Object>> doSelectFromHera(String sql) throws Exception {
        try {
            initHeraConnection();
            Connection connection = heraConnectionPool.getConnection();
            Statement stmt = connection.createStatement(
                    java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = stmt.executeQuery(sql);
            List<Map<String, Object>> list = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> map = rowToMap(rs, rs.getRow());
                list.add(map);
            }
            heraConnectionPool.returnConnection(connection);
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
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

    public void close() throws Exception {
        if (mysqlConnectionPool != null) {
            mysqlConnectionPool.closeConnectionPool();
            heraConnectionPool = null;
        }
        if (heraConnectionPool != null) {
            heraConnectionPool.closeConnectionPool();
            heraConnectionPool = null;
        }
    }

    public void initLineageTable(long tableId, String table, String database) throws Exception {
        doInsert("insert into data_lineage.data_lineage_table " +
                "(`table_id`,`table_name`,`database_name`) " +
                "values('" + tableId + "','" + table + "','" + database + "')");
    }

    public void initLineageColumn(long tableId, String tableName, long columnId, String columnName) throws Exception {
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
            db.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
