package io.confluent.connect.hdfs.orc;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.Format;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;

public class OrcFormat implements Format {
    public RecordWriterProvider getRecordWriterProvider() {
        return new OrcRecordWriterProvider();
    }

    public SchemaFileReader getSchemaFileReader(AvroData avroData) {
        return new OrcFileReader(avroData);
    }

    public HiveUtil getHiveUtil(HdfsSinkConnectorConfig config, AvroData avroData, HiveMetaStore hiveMetaStore) {
        return new OrcHiveUtil(config, avroData, hiveMetaStore);
    }
}
