package com.dfire.products.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description :
 * @Author ： HeGuoZi
 * @Date ： 2:39 PM 2018/12/14
 * @Modified :
 */
public class HeraUtil {

    private static DBUtil dbUtil = new DBUtil();

    static class HeraGroupEntity {
        int    id;
        int    parent;
        String configs;
    }

    private static Map<Integer, HeraGroupEntity> heraGroupEntityMap;

    private static void convertResources(String resource, List<Map<String, String>> tempRes) {
        if (StringUtils.isNotBlank(resource)) {
            try {
                JSONObject resArray = JSONObject.parseObject(resource);
                Map<String, String> itemMap = JSONObject.toJavaObject(resArray, Map.class);
                tempRes.add(itemMap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void recursionHeraGroup(Integer groupId, List<Map<String, String>> tempRes) {
        if (heraGroupEntityMap == null) {
            heraGroupEntityMap = new HashMap<>();
            List<Map<String, Object>> rs;
            try {
                rs = dbUtil.doSelectFromHera("select id,parent,configs from lineage.hera_group where existed=1");
                for (Map<String, Object> map : rs) {
                    HeraGroupEntity tmp = new HeraGroupEntity();
                    tmp.configs = (String) map.get("configs");
                    tmp.parent = (Integer) map.get("parent");
                    heraGroupEntityMap.put((Integer) map.get("id"), tmp);
                }
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
        try {
            HeraGroupEntity heraGroupEntity = heraGroupEntityMap.get(groupId);
            convertResources(heraGroupEntity.configs, tempRes);
            if (heraGroupEntity.parent != 0) {
                recursionHeraGroup(heraGroupEntity.parent, tempRes);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 读取hera库中hive脚本，然后解析
     */
//    public static void main(String[] args) {
    public static List<HeraJobEntity> getHeraJobList() {
        try {
            List<HeraJobEntity> heraJobList = new ArrayList<>();
            List<Map<String, Object>> rs = dbUtil.doSelectFromHera("select id,group_id,configs,script from lineage.hera_job where (run_type='hive' or run_type='spark') and auto=1;");
            for (Map<String, Object> map : rs) {
                HeraJobEntity tmp = new HeraJobEntity();
                tmp.id = (long) map.get("id");
                tmp.groupId = (Integer) map.get("group_id");
                tmp.configs = (String) map.get("configs");
                tmp.script = (String) map.get("script");
                convertResources(tmp.configs, tmp.configList);
                recursionHeraGroup(tmp.groupId, tmp.configList);
                heraJobList.add(tmp);
            }
            for (HeraJobEntity heraJobEntity : heraJobList) {
                for (Map<String, String> map : heraJobEntity.configList) {
                    for (String key : map.keySet()) {
                        String val = "${" + key + "}";
                        heraJobEntity.script = heraJobEntity.script.replace(val, map.get(key));
                    }
                }
            }
            return heraJobList;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                dbUtil.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
