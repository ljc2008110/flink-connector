package org.apache.flink.streaming.connectors.mongodb.sink;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.mongodb.client.MongoClientProvider;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public abstract class MongodbBaseSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
    private final MongodbSinkConf mongodbSinkConf;
    private transient MongoClientProvider mongoClientProvider;
    private transient MongoClient client;
    private transient List<Document> batch;

    protected MongodbBaseSinkFunction(MongodbSinkConf mongodbSinkConf) {
        this.mongodbSinkConf = mongodbSinkConf;
        this.mongoClientProvider = new MongoClientProvider(this.mongodbSinkConf.getUri(),
                this.mongodbSinkConf.getMaxConnectionIdleTime());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.client = mongoClientProvider.getMongoClient();
        this.batch = new ArrayList();
    }

    @Override
    public void close() throws Exception {
        flush();
        super.close();
        mongoClientProvider.closeClient();
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        this.batch.add(invokeDocument(value, context));
        if (this.batch.size() >= this.mongodbSinkConf.getBatchSize()) {
            flush();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) {
    }

    private void flush() {
        if (this.batch.isEmpty()) {
            return;
        }
        MongoDatabase mongoDatabase = this.client.getDatabase(this.mongodbSinkConf.getDatabase());

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(this.mongodbSinkConf.getCollection());
        mongoCollection.insertMany(this.batch);

        this.batch.clear();
    }

    abstract Document invokeDocument(IN paramIN, Context paramContext) throws Exception;
}
