package com.dfire.products.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description :
 * @Author ： HeGuoZi
 * @Date ： 3:31 PM 2018/12/7
 * @Modified :
 */
public class HiveClient implements Closeable {

//    private static final String META_STORE_URIS = "thrift://hadoop1008.daily.2dfire.info:9083";

    private static final String META_STORE_URIS = "thrift://hadoop1160.prod.2dfire.info:9083,thrift://hadoop1161.prod.2dfire.info:9083";

    private static final Logger LOG = LoggerFactory.getLogger(HiveClient.class);

    private static IMetaStoreClient msc;

    private static DBUtil dbUtil = new DBUtil();

    public static IMetaStoreClient getMetaStore() throws HiveException, MetaException {
        if (msc == null) {
            HiveConf hiveConf = new HiveConf();
            hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, META_STORE_URIS);
            Hive hive = Hive.get(hiveConf);
            msc = hive.getMSC();
        }
        return msc;
    }

    public static Meta getMeta(String db, String tableName) throws TException, HiveException {
        List<String> nameList = new ArrayList<>();
        List<FieldSchema> fields = getMetaStore().getFields(db, tableName);
        for (FieldSchema field : fields) {
            nameList.add(field.getName());
        }
        Table table = getMetaStore().getTable(db, tableName);
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        String[] columns = nameList.toArray(new String[nameList.size()]);
        String delimiter = table.getSd().getSerdeInfo().getParameters().get("field.delim");

        return new Meta(columns, delimiter, partitionKeys.get(0).getName());
    }

    @Override
    public void close() {
        if (msc != null) {
            msc.close();
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Meta implements Serializable {
        private String[] columns;
        private String   delimiter;
        private String   partitionKey;
    }

    /**
     * 统计hive库表字段信息到mysql
     */
    public static void main(String[] args) {
        IMetaStoreClient client;
        List<String> databases = null;
        int databasesNum;
        int tableNum = 0;
        int columnNum = 0;
        long now = System.currentTimeMillis();
        try {
            client = getMetaStore();
            databases = client.getAllDatabases();
            databasesNum = databases.size();
            for (String database : databases) {
                List<String> allTables = client.getAllTables(database);
                for (String table : allTables) {
                    List<FieldSchema> fields = client.getFields(database, table);
                    dbUtil.initLineageTable(++tableNum, table, database);
                    System.out.println("TableNo:" + tableNum + " FieldsNum:" + fields.size());
                    for (FieldSchema field : fields) {
                        dbUtil.initLineageColumn(tableNum, table, ++columnNum, field.getName());
                    }
                }
            }
            System.out.println("Time Used:" + (System.currentTimeMillis() - now) / 1000.0 + "s");
            System.out.println("databasesNum:" + databasesNum + " tableNum:" + tableNum + " columnNum:" + columnNum);

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(databases);
    }

}
