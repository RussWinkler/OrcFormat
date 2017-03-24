package io.confluent.connect.hdfs.orc;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrcRecordWriterProvider implements RecordWriterProvider {

    private static final Logger log = LoggerFactory.getLogger(OrcRecordWriterProvider.class);
    private final static String EXTENSION = ".orc";

    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter<SinkRecord> getRecordWriter(Configuration conf, String fileName, SinkRecord record, final AvroData avroData) throws IOException {
        final org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(record.valueSchema());
        final OrcConverter converter = new OrcConverter(avroSchema);
        final Writer orcWriter = OrcFile.createWriter(new Path(fileName),
                OrcFile.writerOptions(conf)
                        .setSchema(converter.getOrcSchema()));

        return new RecordWriter<SinkRecord>(){
            @Override
            public void write(SinkRecord record) throws IOException {
                log.trace("Sink record: {}", record.toString());

                Object value = avroData.fromConnectData(record.valueSchema(), record.value());
                orcWriter.addRowBatch(converter.getRowBatch((GenericRecord) value));
            }

            @Override
            public void close() throws IOException {
                orcWriter.close();
            }
        };
    }
}
