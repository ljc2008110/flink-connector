package org.apache.flink.streaming.connectors.mongodb.sink;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public class MongodbDynamicTableSink implements DynamicTableSink {
    private final MongodbSinkConf mongodbSinkConf;
    private final ResolvedSchema tableSchema;

    public MongodbDynamicTableSink(MongodbSinkConf mongodbSinkConf, ResolvedSchema tableSchema) {
        this.mongodbSinkConf = mongodbSinkConf;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        // I、-U、+U、D
        validatePrimaryKey(changelogMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(
                ChangelogMode.insertOnly().equals(requestedMode)
                        || tableSchema.getPrimaryKey().isPresent(),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // 初始化数据结构转换器，可以将二进制的数据转换成flink可操作的Row
        DataStructureConverter converter = context.createDataStructureConverter(this.tableSchema.toPhysicalRowDataType());
        List<String> pks = this.tableSchema.getPrimaryKey().get().getColumns();
        return SinkFunctionProvider.of(new MongodbUpsertSinkFunction(this.mongodbSinkConf, converter, pks));
    }

    @Override
    public DynamicTableSink copy() {
        return new MongodbDynamicTableSink(this.mongodbSinkConf, this.tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }
}
