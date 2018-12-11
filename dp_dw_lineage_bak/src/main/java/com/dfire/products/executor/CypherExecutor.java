package com.dfire.products.executor;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by winterhanbing on 2015/8/13.
 */
public interface CypherExecutor {

    Iterator<Map<String, Object>> query(String statement, Map<String, Object> params);

    int exec(String sql);

    ResultSet execQuery(String sql);

    int exec(String sql, List<Object> objs);
}
