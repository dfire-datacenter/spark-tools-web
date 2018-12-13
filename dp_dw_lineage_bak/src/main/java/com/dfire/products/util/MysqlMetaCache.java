package com.dfire.products.util;

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description :Cache data from mysql
 * @Author ： HeGuoZi
 * @Date ： 10:38 AM 2018/12/11
 * @Modified :
 */
@Data
public class MysqlMetaCache {

    public Map<String, Integer> tableInfo   = new HashMap<>(16384);
    public Map<Integer, String> tableInfoId = new HashMap<>(16384);
    public Map<String, Integer> columnInfo  = new HashMap<>(262144);
    private static DBUtil dbUtil;

    static {
        try {
            dbUtil = new DBUtil();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public MysqlMetaCache() {
        initCacheInfo();
    }

    public void initCacheInfo() {
        try {
            if (tableInfo.size() == 0) {
                List<Map<String, Object>> rs = dbUtil.doSelect("select * from data_lineage.data_lineage_table");
                for (Map map : rs) {
                    tableInfo.put(map.get("database_name") + "." + map.get("table_name"),
                            Integer.parseInt(map.get("table_id").toString()));
                    tableInfoId.put(Integer.parseInt(map.get("table_id").toString()),
                            map.get("database_name") + "." + map.get("table_name"));
                }
                rs = dbUtil.doSelect("select * from data_lineage.data_lineage_column");
                for (Map map : rs) {
                    columnInfo.put(tableInfoId.get(Integer.parseInt(map.get("table_id").toString())) + "." + map.get("column_name"),
                            Integer.parseInt(map.get("column_id").toString()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
