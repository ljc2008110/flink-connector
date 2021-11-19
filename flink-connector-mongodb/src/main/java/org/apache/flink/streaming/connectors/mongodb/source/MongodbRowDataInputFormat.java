/*
 * Copyright(c)2002-2021, 北京意锐新创科技有限公司
 * 项目名称: flink-connector
 * Date: 2021/11/19
 * Author: linjinchun(kevin)
 */
package org.apache.flink.streaming.connectors.mongodb.source;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.streaming.connectors.mongodb.client.MongoClientProvider;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * <p></p>
 *
 * @author: Kevin.Lin
 * @create: 2021/11/19 9:30
 * @since: v1.0.0
 */
public class MongodbRowDataInputFormat extends RichInputFormat<RowData, InputSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbRowDataInputFormat.class);
    private MongodbSourceConf mongodbSourceConf;
    private transient MongoClientProvider clientProvider;
    private transient MongoClient mongoClient;
    private transient MongoCursor cursor;
    private transient boolean hasNext;


    public MongodbRowDataInputFormat(MongodbSourceConf mongodbSourceConf) {
        this.mongodbSourceConf = mongodbSourceConf;
        clientProvider = new MongoClientProvider(this.mongodbSourceConf.getUri(),
                this.mongodbSourceConf.getMaxConnectionIdleTime());
    }

    @Override
    public void configure(Configuration configuration) {
        // do nothing here
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public InputSplit[] createInputSplits(int i) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return null;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public RowData nextRecord(RowData rowData) throws IOException {
        return null;
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        this.mongoClient = this.clientProvider.getMongoClient();
        MongoDatabase database = this.mongoClient.getDatabase(this.mongodbSourceConf.getDatabase());
        MongoCollection collection = database.getCollection(this.mongodbSourceConf.getCollection());
        FindIterable rstIt = collection.find();
        this.cursor = rstIt.cursor();
        this.hasNext = this.cursor.hasNext();
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(this.mongoClient)) {
            clientProvider.closeClient();
        }
    }
}
