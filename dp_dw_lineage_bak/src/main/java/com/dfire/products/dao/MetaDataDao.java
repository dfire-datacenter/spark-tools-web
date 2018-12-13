package com.dfire.products.dao;

import com.dfire.products.bean.ColumnNode;
import com.dfire.products.bean.TableNode;
import com.dfire.products.exception.DBException;
import com.dfire.products.util.Check;
import com.dfire.products.util.DBUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 元数据dao
 *
 * @author heguozi
 */
public class MetaDataDao {

    private DBUtil dbUtil;

    {
        try {
            dbUtil = new DBUtil();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取表+字段信息
     */
    public List<ColumnNode> getColumn(String db, String table) {
        String sqlWhere = "table_name='" + table + "'" + (Check.isEmpty(db) ? " " : (" and database_name='" + db + "'"));
        List<ColumnNode> colList = new ArrayList<>();
        String sql = "SELECT rc.column_id,rc.column_name,rd.table_id,rd.table_name,rd.database_name FROM data_lineage.data_lineage_column rc join " +
                "(SELECT table_id,table_name,database_name from data_lineage.data_lineage_table where " + sqlWhere + ") rd " +
                "on rc.table_id=rd.table_id";
        try {
//            List<Map<String, Object>> rs = dbUtil.doSelect(sql);
//            for (Map<String, Object> map : rs) {
//                ColumnNode column = new ColumnNode();
//                column.setId((Long) map.get("column_id"));
//                column.setColumn((String) map.get("column_name"));
//                column.setTableId((Long) map.get("table_id"));
//                column.setTable((String) map.get("table_name"));
//                column.setDb((String) map.get("database_name"));
//                colList.add(column);
//            }
            return colList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException(sqlWhere, e);
        }
    }

    public List<TableNode> getTable(String db, String table) {
        String sqlWhere = "table_name='" + table + "'" + (Check.isEmpty(db) ? " " : (" and database_name='" + db + "'"));
        List<TableNode> list = new ArrayList<>();
        String sql = "SELECT table_id,table_name,database_name from data_lineage_table where " + sqlWhere + "";
        try {
            List<Map<String, Object>> rs = dbUtil.doSelect(sql);
            for (Map<String, Object> map : rs) {
                TableNode tableNode = new TableNode();
                tableNode.setId((Long) map.get("table_id"));
                tableNode.setTable((String) map.get("table_name"));
                tableNode.setDb((String) map.get("database_name"));
                list.add(tableNode);
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException(sqlWhere, e);
        }
    }

}
