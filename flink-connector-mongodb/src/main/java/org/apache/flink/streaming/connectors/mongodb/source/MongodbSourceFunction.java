package org.apache.flink.streaming.connectors.mongodb.source;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.mongodb.client.MongoClientProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <p></p>
 *
 * @author: Kevin.Lin
 * @create: 2021/11/18 16:55
 * @since: v1.0.0
 */
public class MongodbSourceFunction extends RichSourceFunction {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbSourceFunction.class);

    private final MongodbSourceConf mongodbSourceConf;
    private transient MongoClientProvider mongoClientProvider;
    private final int fieldsCount;
    private final List<String> fieldNames;
    private final List<DataType> fieldTypes;
    private final DebeziumDeserializationSchema<RowData> deserializer;

    protected MongodbSourceFunction(MongodbSourceConf mongodbSourceConf, int fieldsCount,
                                    List<String> fieldNames, List<DataType> fieldTypes,
                                    DebeziumDeserializationSchema<RowData> deserializer) {
        this.mongodbSourceConf = mongodbSourceConf;
        this.fieldsCount = fieldsCount;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.deserializer = deserializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.mongoClientProvider = new MongoClientProvider(this.mongodbSourceConf.getUri(),
                this.mongodbSourceConf.getMaxConnectionIdleTime());
        LOG.info("---------------- init success, clientProvider: {}", this.mongoClientProvider);
    }

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        MongoDatabase database = this.mongoClientProvider.getMongoClient().getDatabase(this.mongodbSourceConf.getDatabase());
        MongoCollection docs = database.getCollection(this.mongodbSourceConf.getCollection());
        FindIterable fit = docs.find(BsonDocument.class);
        MongoCursor<BsonDocument> it = fit.iterator();
        while (it.hasNext()) {
            BsonDocument doc = it.next();
            sourceContext.collect(deserializer.deserialize(doc));
        }
    }

    @Override
    public void cancel() {
        mongoClientProvider.closeClient();
    }

}
