package org.apache.flink.streaming.connectors.mongodb.source;

import org.apache.flink.streaming.connectors.mongodb.MongodbConf;

/**
 * <p></p>
 *
 * @author: Kevin.Lin
 * @create: 2021/11/18 16:55
 * @since: v1.0.0
 */
public class MongodbSourceConf extends MongodbConf {

    public MongodbSourceConf(String database, String collection, String uri, int maxConnectionIdleTime) {
        super(database, collection, uri, maxConnectionIdleTime);
    }

    @Override
    public String toString() {
        return "MongodbSourceConf{" + super.toString() + '}';
    }
}
