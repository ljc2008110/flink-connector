package org.apache.flink.streaming.connectors.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public class Datagen2Mongodb {

    public static void main(String args[]) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        String mongoServ = "mongodm-0.mongodm-headless.base.svc.cluster.local";
        String mongoUser = "flinkuser";
        String mongoPswd = "Inspiry2021";

        String sourceSql = "CREATE TABLE datagen (\n" +
                " id INT,\n" +
                " name STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.name.length'='10'\n" +
                ")";
        String ctSql = "create table device_connect_history_stat_mongo (\n" +
                "  _id STRING,\n" +
                "  _class STRING,\n" +
                "  type STRING,\n" +
                "  serialNum STRING,\n" +
                "  `time` TIMESTAMP_LTZ(0),\n" +
                "  receiveTime TIMESTAMP_LTZ(0),\n" +
                "  msg STRING,\n" +
                "  channelId STRING,\n" +
                "  PRIMARY KEY (_id) NOT ENFORCED\n" +
                ") with (\n" +
                "  'connector' = 'mongodb-cdc',\n" +
                "  'hosts' = 'mongodm-0.mongodm-headless.base.svc.cluster.local:27017',\n" +
                "  'username' = 'flinkuser',\n" +
                "  'password' = 'Inspiry2021',\n" +
                "  'database' = 'dm_admin_logs',\n" +
                "  'collection' = 'device_connect_history_stat',\n" +
                "  'connection.options' = 'replicaSet=rs0',\n" +
                "  'errors.tolerance' = 'all',\n" +
                "  'errors.log.enable' = 'true'\n" +
                ")";
        String sinkSql = "CREATE TABLE mongoddb (\n" +
                "  _id STRING,\n" +
                "  id STRING,\n" +
                "  `name` STRING,\n" +
                "  PRIMARY KEY (_id) NOT ENFORCED \n" +
                ") WITH (\n" +
                "  'connector' = 'mongodb',\n" +
                "  'database'='dm_admin_logs',\n" +
                "  'collection'='flink_mongo_sink_test',\n" +
                "  'uri'='mongodb://" + mongoUser + ":" +
                  mongoPswd + "@" + mongoServ + ":27017/?replicaSet=rs0&authSource=admin',\n" +
                "  'maxConnectionIdleTime'='20000',\n" +
                "  'batchSize'='1'\n" +
                ")";
        String insertSql = "insert into mongoddb " +
                "select serialNum, serialNum,type " +
                "from device_connect_history_stat_mongo";

        tableEnvironment.executeSql(ctSql);
        tableEnvironment.executeSql(sinkSql);
        tableEnvironment.executeSql(insertSql);
    }
}
