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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Description :
 * @Author ： HeGuoZi
 * @Date ： 3:31 PM 2018/12/7
 * @Modified :
 */
public class HiveClient implements Closeable {

    private static final String META_STORE_URIS = PropertyFileUtil.getProperty("hive.metastore.uris");

    private static final Logger LOG = LoggerFactory.getLogger(HiveClient.class);

    private static IMetaStoreClient msc;

    private static DBUtil dbUtil = new DBUtil();

    public static LinkedBlockingQueue<IMetaStoreClient> clientPool = new LinkedBlockingQueue<>(10);

    private static MagicSnowFlake msf = new MagicSnowFlake(1, 1);

    public static ConcurrentHashMap<String, List<String>> tableColumnInfo = new ConcurrentHashMap<>(16384);


    public static IMetaStoreClient getMetaStore() throws HiveException, MetaException {
        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, META_STORE_URIS);
        Hive hive = Hive.get(hiveConf);
        return hive.getMSC();
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
        if (clientPool.size() != 0) {
            for (IMetaStoreClient tmpClient : clientPool) {
                tmpClient.close();
            }
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

    private static void initClientPool() {
        if (clientPool.size() == 0) {
            int size = 10;
            ExecutorService threadPool = Executors.newFixedThreadPool(size);
            for (int i = 0; i < size; i++) {
                try {
                    threadPool.submit(() -> {
                        try {
                            IMetaStoreClient client = getMetaStore();
                            clientPool.put(client);
                        } catch (InterruptedException | HiveException | MetaException e) {
                            e.printStackTrace();
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 初始化所有表的字段信息 按照顺序
     */
    public static Map<String, List<String>> initAndGetTableColumnInfo() {
        if (tableColumnInfo.size() != 0) {
            tableColumnInfo.clear();
        }
        initClientPool();
        try {
            IMetaStoreClient outClient = getMetaStore();
            List<String> databases = outClient.getAllDatabases();
            int dbNum = 0;
            for (String database : databases) {
                System.out.println("No:" + (++dbNum) + ";DbName:" + database);
                List<String> allTables = outClient.getAllTables(database);
                allTables.parallelStream().forEach(e -> {
                    try {
                        IMetaStoreClient client = clientPool.take();
                        List<FieldSchema> fields = client.getFields(database, e);
                        List<String> columns = new ArrayList<>();
                        fields.forEach(field -> columns.add(field.getName()));
                        tableColumnInfo.put(database + "." + e, columns);
                        clientPool.put(client);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                });
            }
            outClient.close();
            if (clientPool.size() != 0) {
                for (IMetaStoreClient tmpClient : clientPool) {
                    tmpClient.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tableColumnInfo;
    }

    public static void initMetaInfoToMysql() {
        if (tableColumnInfo.size() != 0) {
            tableColumnInfo.clear();
        }
        initClientPool();
        List<String> databases = null;
        int databasesNum;
        AtomicInteger columnNum = new AtomicInteger(0);
        AtomicInteger tableNum = new AtomicInteger(0);
        long now = System.currentTimeMillis();
        try {
            IMetaStoreClient outClient = getMetaStore();
            databases = outClient.getAllDatabases();
            databasesNum = databases.size();
            for (String database : databases) {
                List<String> allTables = outClient.getAllTables(database);
                allTables.parallelStream().forEach(e -> {
                    try {
                        IMetaStoreClient client = clientPool.take();
                        List<FieldSchema> fields = client.getFields(database, e);
                        List<String> columns = new ArrayList<>();
                        fields.forEach(field -> columns.add(field.getName()));
                        tableColumnInfo.put(database + "." + e, columns);
                        long tableId = msf.nextId();
                        dbUtil.initLineageTable(tableId, e, database);
                        tableNum.getAndIncrement();
                        fields.parallelStream().forEach(f -> {
                            try {
                                dbUtil.initLineageColumn(tableId, e, msf.nextId(), f.getName());
                                columnNum.getAndIncrement();
                                System.out.println("TableNo:" + tableNum.get() + " FieldsNum:" + columnNum.get());
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        });
                        clientPool.put(client);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                });
            }
            outClient.close();
            if (clientPool.size() != 0) {
                for (IMetaStoreClient tmpClient : clientPool) {
                    tmpClient.close();
                }
            }
            dbUtil.close();
            System.out.println("Time Used:" + (System.currentTimeMillis() - now) / 1000.0 + "s");
            System.out.println("databasesNum:" + databasesNum + " tableNum:" + tableNum + " columnNum:" + columnNum);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}

