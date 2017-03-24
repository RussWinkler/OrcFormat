package io.confluent.connect.hdfs.orc;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.DataWriter;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.hive.HiveTestBase;
import io.confluent.connect.hdfs.hive.HiveTestUtils;
import io.confluent.connect.hdfs.hive.HiveUtil;
import io.confluent.connect.hdfs.partitioner.Partitioner;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class OrcHiveUtilTest extends HiveTestBase {
    private HiveUtil hive;

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(HdfsSinkConnectorConfig.FORMAT_CLASS_CONFIG, OrcFormat.class.getName());
        return props;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Map<String, String> props = createProps();
        HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);
        hive = new OrcHiveUtil(connectorConfig, avroData, hiveMetaStore);
    }

    @Test
    public void testCreateTable() throws Exception {
        prepareData(TOPIC, PARTITION);
        Partitioner partitioner = HiveTestUtils.getPartitioner();

        Schema schema = OrcTestUtils.createSchema();
        hive.createTable(hiveDatabase, TOPIC, schema, partitioner);
        String location = "partition=" + String.valueOf(PARTITION);
        hiveMetaStore.addPartition(hiveDatabase, TOPIC, location);

        List<String> expectedColumnNames = new ArrayList<>();
        for (Field field: schema.fields()) {
            expectedColumnNames.add(field.name());
        }

        Table table = hiveMetaStore.getTable(hiveDatabase, TOPIC);
        List<String> actualColumnNames = new ArrayList<>();
        for (FieldSchema column: table.getSd().getCols()) {
            actualColumnNames.add(column.getName());
        }

        assertEquals(expectedColumnNames, actualColumnNames);
        List<FieldSchema> partitionCols = table.getPartitionKeys();
        assertEquals(1, partitionCols.size());
        assertEquals("partition", partitionCols.get(0).getName());

        String[] expectedResult = {"String Test", String.valueOf(Long.MAX_VALUE), "true", "16", "12.2", "12.2"};
        String result = HiveTestUtils.runHive(hiveExec, "SELECT * FROM " + TOPIC);
        String[] rows = result.split("\n");
        // Only 6 of the 7 records should have been delivered due to flush_size = 3
        assertEquals(6, rows.length);
        for (String row: rows) {
            String[] parts = OrcTestUtils.parseOutput(row);
            for (int j = 0; j < expectedResult.length; ++j) {
                assertEquals(expectedResult[j], parts[j]);
            }
        }
    }

    private void prepareData(String topic, int partition) throws Exception {
        TopicPartition tp = new TopicPartition(topic, partition);
        DataWriter hdfsWriter = createWriter(context, avroData);
        hdfsWriter.recover(tp);
        String key = "key";
        Schema schema = OrcTestUtils.createSchema();
        Struct record = OrcTestUtils.createRecord(schema);

        Collection<SinkRecord> sinkRecords = new ArrayList<>();
        for (long offset = 0; offset < 7; offset++) {
            SinkRecord sinkRecord =
                    new SinkRecord(topic, partition, Schema.STRING_SCHEMA, key, schema, record, offset);
            sinkRecords.add(sinkRecord);
        }
        hdfsWriter.write(sinkRecords);
        hdfsWriter.close(assignment);
        hdfsWriter.stop();
    }

    private DataWriter createWriter(SinkTaskContext context, AvroData avroData) {
        Map<String, String> props = createProps();
        HdfsSinkConnectorConfig connectorConfig = new HdfsSinkConnectorConfig(props);
        return new DataWriter(connectorConfig, context, avroData);
    }
}
