package org.apache.flink.streaming.connectors.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public class MongodbSourceTest {

    public static void main(String args[]) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        String mongoServ = "mongodm-0.mongodm-headless.base.svc.cluster.local";
        String mongoUser = "flinkuser";
        String mongoPswd = "Inspiry2021";

        String sourceSql = "CREATE TABLE mongoddb (\n" +
                "  id INT,\n" +
                "  name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'mongodb',\n" +
                "  'database'='dm_admin_logs',\n" +
                "  'collection'='flink_mongo_sink_test',\n" +
                "  'uri'='mongodb://" + mongoUser + ":" +
                  mongoPswd + "@" + mongoServ + ":27017/?replicaSet=rs0&authSource=admin',\n" +
                "  'maxConnectionIdleTime'='20000' \n" +
                ")";
        sourceSql = "CREATE TABLE mongoddb (\n" +
                " _id STRING, \n" +
                " _class STRING, \n" +
                " type STRING, \n" +
                " serialNum STRING, \n" +
                " `time` TIMESTAMP_LTZ(0), \n" +
                " receiveTime TIMESTAMP_LTZ(0), \n" +
                " msg STRING, \n" +
                " channelId STRING \n" +
                ") WITH (\n" +
                "  'connector' = 'mongodb',\n" +
                "  'database'='dm_admin_logs',\n" +
                "  'collection'='device_connect_history_stat',\n" +
                "  'uri'='mongodb://" + mongoUser + ":" +
                mongoPswd + "@" + mongoServ + ":27017/?replicaSet=rs0&authSource=admin',\n" +
                "  'maxConnectionIdleTime'='20000' \n" +
                ")";

        String selectSql = "select _id,type,serialNum,receiveTime,msg from mongoddb where type='disconnect'";

        tableEnvironment.executeSql(sourceSql);
        tableEnvironment.executeSql(selectSql).print();
    }
}
