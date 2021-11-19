package org.apache.flink.streaming.connectors.mongodb.sink;

import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.bson.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public class MongodbUpsertSinkFunction extends MongodbBaseSinkFunction<RowData> {
    private final DynamicTableSink.DataStructureConverter converter;
    private final List<String> fieldNames;

    public MongodbUpsertSinkFunction(MongodbSinkConf mongodbSinkConf, List<String> fieldNames, DynamicTableSink.DataStructureConverter converter) {
        super(mongodbSinkConf);
        this.fieldNames = fieldNames;
        this.converter = converter;
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
        for (int i = 0; i < this.fieldNames.size(); i++) {
            map.put(this.fieldNames.get(i), row.getField(i));
        }
        return new Document(map);
    }
}
