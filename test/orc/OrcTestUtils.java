package io.confluent.connect.hdfs.orc;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;

public class OrcTestUtils {

    public static Schema createSchema() {
        return SchemaBuilder.struct().name("record").version(1)
                .field("string", Schema.STRING_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .build();
    }

    public static Struct createRecord(Schema schema) {
        return new Struct(schema)
                .put("string", "String Test")
                .put("long", Long.MAX_VALUE)
                .put("boolean", true)
                .put("int", 16)
                .put("float", 12.2f)
                .put("double", 12.2);
    }

    public static Struct[] createRecords(Schema schema) {
        Struct record1 = new Struct(schema)
                .put("string", "String Test 16")
                .put("long", Long.MAX_VALUE)
                .put("boolean", true)
                .put("int", 16)
                .put("float", 12.2f)
                .put("double", 12.2);

        Struct record2 = new Struct(schema)
                .put("string", "String Test 17")
                .put("long", Long.MAX_VALUE)
                .put("boolean", true)
                .put("int", 17)
                .put("float", 12.2f)
                .put("double", 12.2);

        Struct record3 = new Struct(schema)
                .put("string", "String Test 18")
                .put("long", Long.MAX_VALUE)
                .put("boolean", true)
                .put("int", 18)
                .put("float", 12.2f)
                .put("double", 12.2);

        /*
        Struct record1 = new Struct(schema)
                .put("string", STRING_TEST + " 1")
                .put("long", LONG_TEST)
                .put("boolean", BOOLEAN_TEST)
                .put("int", INT_TEST)
                .put("float", FLOAT_TEST)
                .put("double", DOUBLE_TEST);

        Struct record2 = new Struct(schema)
                .put("string", STRING_TEST + " 2")
                .put("long", LONG_TEST - 1l)
                .put("boolean", !BOOLEAN_TEST)
                .put("int", INT_TEST + 1)
                .put("float", FLOAT_TEST + 1f)
                .put("double", DOUBLE_TEST + 1d);

        Struct record3 = new Struct(schema)
                .put("string", STRING_TEST + " 3")
                .put("long", LONG_TEST - 2l)
                .put("boolean", BOOLEAN_TEST)
                .put("int", INT_TEST + 2)
                .put("float", FLOAT_TEST + 2f)
                .put("double", DOUBLE_TEST + 2d);
        */

        ArrayList<Struct> records = new ArrayList<>();
        records.add(record1);
        records.add(record2);
        records.add(record3);
        return records.toArray(new Struct[records.size()]);
    }

    public static String[] parseOutput(String output) {
        return output.split("\t");
    }
}
