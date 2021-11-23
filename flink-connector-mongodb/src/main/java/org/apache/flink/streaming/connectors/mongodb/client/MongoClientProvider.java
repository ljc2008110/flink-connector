/*
 * Copyright(c)2002-2021, 北京意锐新创科技有限公司
 * 项目名称: flink-connector
 * Date: 2021/11/18
 * Author: linjinchun(kevin)
 */
package org.apache.flink.streaming.connectors.mongodb.client;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.annotations.NotThreadSafe;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * <p></p>
 *
 * @author: Kevin.Lin
 * @create: 2021/11/18 13:55
 * @since: v1.0.0
 */
@NotThreadSafe
public class MongoClientProvider {

    private static final Logger LOG = LoggerFactory.getLogger(MongoClientProvider.class);

    private transient MongoClient mongoClient;

    private final String uri;
    private final int maxConnectionIdleTime;

    public MongoClientProvider(String uri, int maxConnectionIdleTime) {
        this.uri = uri;
        this.maxConnectionIdleTime = maxConnectionIdleTime;
    }

    public MongoClient getMongoClient() {
        if (Objects.nonNull(mongoClient)) {
            return mongoClient;
        }

        if (Strings.isNullOrEmpty(uri)) {
            LOG.error("mongodb connect uri is empty.");
            return null;
        }

        this.mongoClient = openMongoClient();

        return this.mongoClient;
    }

    private MongoClient openMongoClient() {
        return new MongoClient(
                new MongoClientURI(this.uri, getOptions(this.maxConnectionIdleTime)));
    }

    private MongoClientOptions.Builder getOptions(int maxConnectionIdleTime) {
        MongoClientOptions.Builder optionsBuilder = new MongoClientOptions.Builder();
        optionsBuilder.maxConnectionIdleTime(maxConnectionIdleTime);
        return optionsBuilder;
    }

    public void closeClient() {
        this.mongoClient.close();
        this.mongoClient = null;
    }
}
