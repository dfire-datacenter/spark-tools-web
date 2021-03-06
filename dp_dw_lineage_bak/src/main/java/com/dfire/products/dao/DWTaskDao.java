package com.dfire.products.dao;

import com.dfire.products.bean.DWTask;
import com.dfire.products.exception.DBException;
import com.dfire.products.util.Check;
import com.dfire.products.util.DBUtil;
import com.dfire.products.util.DateUtil;

import java.util.*;

public class DWTaskDao {

    private DBUtil dbUtil = new DBUtil();

    public List<DWTask> getTask(Date date, int startTaskId, List<Integer> taskIdList) {
        StringBuilder where = new StringBuilder();
        where.append(" where t.state=1 and t.type in ('SHELL','HIVE') and t.id>=").append(startTaskId);
        if (date != null) {
            where.append(" and ").append("t.updateDate>='").append(DateUtil.dateTimeToString(date)).append("' ");
        }
        if (Check.notEmpty(taskIdList)) {
            where.append(" and t.id in (").append(collectionToString(taskIdList)).append(") ");
        }
        String sql = " select t.id,t.name,concat(t.appPath,'/',t.mainClazz) as path,u.name as user,u.mail " +
                " from task t join user u on t.userId=u.id " + where.toString()
                + " order by t.id";
        List<DWTask> colList = new ArrayList<>();
        try {
            List<Map<String, Object>> rs = dbUtil.doSelect(sql);
            for (Map<String, Object> map : rs) {
                DWTask task = new DWTask();
                task.setId((Long) map.get("id"));
                task.setName((String) map.get("name"));
                String path = (String) map.get("path");
                if (Check.notEmpty(path)) {
                    int indexOf = path.indexOf(" ");
                    task.setPath(indexOf > 0 ? path.substring(0, indexOf) : path);
                }
                task.setUser((String) map.get("user"));
                task.setMail((String) map.get("mail"));
                colList.add(task);
            }
            return colList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBException(e);
        } finally {
            try {
                dbUtil.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private String collectionToString(Collection<Integer> coll) {
        StringBuilder sb = new StringBuilder();
        if (Check.notEmpty(coll)) {
            for (Integer string : coll) {
                sb.append(string).append(",");
            }
            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1);
            }
        }
        return sb.toString();
    }

}
