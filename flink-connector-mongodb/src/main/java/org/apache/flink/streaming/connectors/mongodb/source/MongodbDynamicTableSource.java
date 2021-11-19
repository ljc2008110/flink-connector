package org.apache.flink.streaming.connectors.mongodb.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p></p>
 *
 * @author: Kevin.Lin
 * @create: 2021/11/18 16:55
 * @since: v1.0.0
 */
public class MongodbDynamicTableSource implements ScanTableSource {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbDynamicTableSource.class);
    private final MongodbSourceConf mongodbSourceConf;
    private final ResolvedSchema tableSchema;

    public MongodbDynamicTableSource(MongodbSourceConf mongodbSourceConf, ResolvedSchema tableSchema) {
        this.mongodbSourceConf = mongodbSourceConf;
        this.tableSchema = tableSchema;
    }

    @Override
    public DynamicTableSource copy() {
        return new MongodbDynamicTableSource(mongodbSourceConf, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType physicalDataType = (RowType)tableSchema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInfo =
                scanContext.createTypeInformation(tableSchema.toSourceRowDataType());
        DebeziumDeserializationSchema<RowData> deserializer =
                new MongoDBConnectorDeserializationSchema(physicalDataType, typeInfo);

        final SourceFunction<RowData> sourceFunction = new MongodbSourceFunction(this.mongodbSourceConf,
                this.tableSchema.getColumnCount(),
                this.tableSchema.getColumnNames(),
                this.tableSchema.getColumnDataTypes(),
                deserializer);

        LOG.info("---------------------  sourceFunction : {}", sourceFunction);
        return SourceFunctionProvider.of(sourceFunction, true);
    }
}
