package org.apache.flink.streaming.connectors.mongodb.sink;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public class MongodbUpsertSinkFunction extends MongodbBaseSinkFunction<RowData> {
    private final DynamicTableSink.DataStructureConverter converter;
    private final List<String> pks;

    public MongodbUpsertSinkFunction(MongodbSinkConf mongodbSinkConf,
                                     DynamicTableSink.DataStructureConverter converter,
                                     List<String> pks) {
        super(mongodbSinkConf);
        this.converter = converter;
        this.pks = pks;
    }

    /**
     * 将二进制RowData转换成flink可处理的Row，再将Row封装成要插入的Document对象
     *
     * @param value
     * @param context
     * @return
     */
    @Override
    Document invokeDocument(RowData value, Context context) {
        Row row = (Row) this.converter.toExternal(value);
        Map<String, Object> map = new HashMap();
        Set<String> fieldNames = row.getFieldNames(true);
        for (String fieldName : fieldNames) {
            map.put(fieldName, row.getField(fieldName));
        }
        return new Document(map);
    }

    @Override
    boolean existAndUpdate(Document doc) {
        if (pks.isEmpty()) {
            // 没有主键，直接插入，不更新。
            return false;
        }
        Bson filter = Filters.eq(pks.get(0), doc.get(pks.get(0)));
        for (int i = 1; i < pks.size(); i++) {
            filter = Filters.and(filter, Filters.eq(pks.get(i), doc.get(pks.get(i))));
        }

        return Objects.nonNull(this.mongoCollection.findOneAndUpdate(filter, new Document().append("$set", doc)));
    }

    @Override
    protected boolean processOneInsertOrUpdate(Document doc) {
        if (pks.isEmpty()) {
            // 没有主键，直接插入，不更新。
            return false;
        }
        Bson filter = Filters.eq(pks.get(0), doc.get(pks.get(0)));
        for (int i = 1; i < pks.size(); i++) {
            filter = Filters.and(filter, Filters.eq(pks.get(i), doc.get(pks.get(i))));
        }

        this.mongoCollection.findOneAndUpdate(filter, new Document().append("$set", doc), new FindOneAndUpdateOptions().upsert(true));
        return true;
    }
}
