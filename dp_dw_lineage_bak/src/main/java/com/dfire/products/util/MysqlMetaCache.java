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

    public         Map<String, Long> tableInfo   = new HashMap<>(16384);
    public         Map<Long, String> tableInfoId = new HashMap<>(16384);
    public         Map<String, Long> columnInfo  = new HashMap<>(262144);
    private static DBUtil               dbUtil      = new DBUtil();

    public MysqlMetaCache() {
        initCacheInfo();
    }

    public void initCacheInfo() {
        try {
            if (tableInfo.size() == 0) {
                List<Map<String, Object>> rs = dbUtil.doSelect("select * from data_lineage.data_lineage_table");
                for (Map map : rs) {
                    tableInfo.put(map.get("database_name") + "." + map.get("table_name"),
                            Long.parseLong(map.get("table_id").toString()));
                    tableInfoId.put(Long.parseLong(map.get("table_id").toString()),
                            map.get("database_name") + "." + map.get("table_name"));
                }
                rs = dbUtil.doSelect("select * from data_lineage.data_lineage_column");
                for (Map map : rs) {
                    columnInfo.put(tableInfoId.get(Long.parseLong(map.get("table_id").toString())) + "." + map.get("column_name"),
                            Long.parseLong(map.get("column_id").toString()));
                }
            }
            System.out.println("Successfully init Mysql MetaInfo!");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                dbUtil.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
