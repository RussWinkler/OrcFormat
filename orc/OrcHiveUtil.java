package io.confluent.connect.hdfs.orc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.connect.data.Schema;

import java.util.List;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.FileUtils;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveSchemaConverter;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.errors.HiveMetaStoreException;

public class OrcHiveUtil extends HiveUtil {
    public OrcHiveUtil(HdfsSinkConnectorConfig connectorConfig, AvroData avroData, HiveMetaStore hiveMetaStore) {
        super(connectorConfig, avroData, hiveMetaStore);
    }

    @Override
    public void createTable(String database, String tableName, Schema schema, Partitioner partitioner) throws HiveMetaStoreException {
        Table table = constructOrcTable(database, tableName, schema, partitioner);
        hiveMetaStore.createTable(table);
    }

    @Override
    public void alterSchema(String database, String tableName, Schema schema) {
        Table table = hiveMetaStore.getTable(database, tableName);
        List<FieldSchema> columns = HiveSchemaConverter.convertSchema(schema);
        table.setFields(columns);
        hiveMetaStore.alterTable(table);
    }

    private Table constructOrcTable(String database, String tableName, Schema schema, Partitioner partitioner) throws HiveMetaStoreException {
        Table table = new Table(database, tableName);
        table.setTableType(TableType.EXTERNAL_TABLE);
        table.getParameters().put("EXTERNAL", "TRUE");
        String tablePath = FileUtils.hiveDirectoryName(url, topicsDir, tableName);
        table.setDataLocation(new Path(tablePath));
        table.setSerializationLib(getHiveOrcSerde());
        try {
            table.setInputFormatClass(getHiveOrcInputFormat());
            table.setOutputFormatClass(getHiveOrcOutputFormat());
        } catch (HiveException e) {
            throw new HiveMetaStoreException("Cannot find input/output format:", e);
        }
        List<FieldSchema> columns = HiveSchemaConverter.convertSchema(schema);
        table.setFields(columns);
        table.setPartCols(partitioner.partitionFields());
        return table;
    }

    private String getHiveOrcInputFormat() {
        return "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    }

    private String getHiveOrcOutputFormat() {
        return "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
    }

    private String getHiveOrcSerde() {
        return "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
    }
}
