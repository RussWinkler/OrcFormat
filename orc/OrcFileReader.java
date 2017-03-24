package io.confluent.connect.hdfs.orc;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.SchemaFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.Collection;

import static org.apache.orc.OrcFile.readerOptions;


public class OrcFileReader implements SchemaFileReader {

    private AvroData avroData;

    public OrcFileReader(AvroData avroData) {
        this.avroData = avroData;
    }

    @Override
    public Schema getSchema(Configuration conf, Path path) throws IOException {
        Reader reader = OrcFile.createReader(path, readerOptions(conf));
        TypeDescription orcSchema = reader.getSchema();
        OrcConverter converter = new OrcConverter(orcSchema);

        return avroData.toConnectSchema(converter.getAvroSchema());
    }

    /*
     * This method has a dependency issue as long as the hive-exec dependency in hive-cli is updated
     * to version 2.0.1 or greater.  That update will require a major rework to migrate away from the
     * orc classes located in org.apache.hive and use the classes exclusively located in org.apache.orc
     * once the Apache reworking of the orc tools is complete.
     */
    @Override
    public Collection<Object> readData(Configuration conf, Path path) throws IOException {
        throw new OrcFileReaderNotImplementedException("readData method not implemented yet");
    }

    class OrcFileReaderNotImplementedException extends IOException {
        public OrcFileReaderNotImplementedException() {}
        public OrcFileReaderNotImplementedException(String message) {
            super(message);
        }
    }
}