package io.confluent.connect.hdfs.orc;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class OrcConverter {
    private TypeDescription orcSchema;
    private Schema avroSchema;
    private List<String> supportedFields = new ArrayList<>(Arrays.asList("string","bigint", "float", "double", "boolean",
                                                                         "int"));

    private final String DEFAULT_AVRO_NAME = "DefaultAvroSchema";

    public OrcConverter(TypeDescription orcSchema) throws OrcConverterUnsupportedFieldException {
        this.orcSchema = orcSchema;
        convertOrcToAvro();
        checkUnsupportedFields();
    }

    public OrcConverter(Schema avroSchema) throws OrcConverterUnsupportedFieldException {
        this.avroSchema = avroSchema;
        convertAvroToOrc();
        checkUnsupportedFields();
    }

    public VectorizedRowBatch getRowBatch(GenericRecord record) throws OrcConverterUnsupportedFieldException {
        VectorizedRowBatch batch = orcSchema.createRowBatch();
        List<org.apache.avro.Schema.Field> fields = record.getSchema().getFields();
        if(fields != null) {
            batch.size++;
            for (int i = 0; i < fields.size(); i++) {
                org.apache.avro.Schema.Field field = fields.get(i);
                org.apache.avro.Schema fieldSchema = field.schema();
                Object o = record.get(field.name());
                switch(fieldSchema.getType().getName()) {
                    case "float":
                        DoubleColumnVector floatColumn = (DoubleColumnVector) batch.cols[i];
                        floatColumn.vector[0] = (float) o;
                        break;
                    case "double":
                        DoubleColumnVector doubleColumn = (DoubleColumnVector) batch.cols[i];
                        doubleColumn.vector[0] = (double) o;
                        break;
                    case "boolean":
                        LongColumnVector booleanColumn = (LongColumnVector) batch.cols[i];
                        booleanColumn.vector[0] = ((boolean) o == true ? 1l : 0l);
                        break;
                    case "int":
                        LongColumnVector intColumn = (LongColumnVector) batch.cols[i];
                        intColumn.vector[0] =  new Long((int) o);
                        break;
                    case "long":
                        LongColumnVector longColumn = (LongColumnVector) batch.cols[i];
                        longColumn.vector[0] = (Long) o;
                        break;
                    case "string":
                        BytesColumnVector stringColumn = (BytesColumnVector) batch.cols[i];
                        stringColumn.setVal(0, o.toString().getBytes(), 0, o.toString().getBytes().length);
                        break;
                    default:
                        throw new OrcConverterUnsupportedFieldException("Attempted to convert an unsupported schema field");
                }
            }
        }

        return batch;
    }

    public TypeDescription getOrcSchema() {
        return this.orcSchema;
    }

    public Schema getAvroSchema() {
        return this.avroSchema;
    }

    private boolean isSupportedCategory(TypeDescription.Category category) {
        if(!supportedFields.contains(category.getName()))
            return false;

        return true;
    }

    private void convertAvroToOrc() throws OrcConverterUnsupportedFieldException {
        orcSchema = TypeDescription.createStruct();
        Schema.Type avroFieldsType = avroSchema.getType();
        if(avroFieldsType != Schema.Type.RECORD)
            throw new OrcConverterUnsupportedFieldException("Expected Avro RECORD Struct!");
        List<Schema.Field> fields = avroSchema.getFields();

        for(Schema.Field field : fields) {
            Schema.Type avroFieldType = field.schema().getType();
            String fieldName = field.name();
            switch (avroFieldType) {
                case FLOAT:
                    orcSchema.addField(fieldName, TypeDescription.createFloat());
                    break;
                case DOUBLE:
                    orcSchema.addField(fieldName, TypeDescription.createDouble());
                    break;
                case BOOLEAN:
                    orcSchema.addField(fieldName, TypeDescription.createBoolean());
                    break;
                case INT:
                    orcSchema.addField(fieldName, TypeDescription.createInt());
                    break;
                case LONG:
                    orcSchema.addField(fieldName, TypeDescription.createLong());
                    break;
                case STRING:
                case ENUM:
                    orcSchema.addField(fieldName, TypeDescription.createString());
                    break;
                default:
                    throw new OrcConverterUnsupportedFieldException("Avro type " + avroFieldType.getName() + " not recognized or not supported");
            }
        }
    }

    private void convertOrcToAvro() {
        avroSchema = Schema.createRecord(DEFAULT_AVRO_NAME, null, null, false);
        List<Schema.Field> fields = new LinkedList<>();
        List<TypeDescription> children = orcSchema.getChildren();
        List<String> fieldNames = orcSchema.getFieldNames();
        for(int i = 0 ; i < children.size() ; i++) {
            String fieldName = fieldNames.get(i);
            String fieldCategory = children.get(i).getCategory().getName();
            switch (fieldCategory) {
                case "float":
                    fields.add(new Schema.Field(fieldName, Schema.create(Schema.Type.FLOAT), null, null));
                    break;
                case "double":
                    fields.add(new Schema.Field(fieldName, Schema.create(Schema.Type.DOUBLE), null, null));
                    break;
                case "boolean":
                    fields.add(new Schema.Field(fieldName, Schema.create(Schema.Type.BOOLEAN), null, null));
                    break;
                case "int":
                    fields.add(new Schema.Field(fieldName, Schema.create(Schema.Type.INT), null, null));
                    break;
                case "bigint":
                    fields.add(new Schema.Field(fieldName, Schema.create(Schema.Type.LONG), null, null));
                    break;
                case "string":
                case "enum":
                    fields.add(new Schema.Field(fieldName, Schema.create(Schema.Type.STRING), null, null));
                    break;
                default:
                    throw new OrcConverterUnsupportedFieldException("ORC field " + fieldCategory + " not recognized or not supported");
            }
        }
        avroSchema.setFields(fields);
    }

    private void checkUnsupportedFields() throws OrcConverterUnsupportedFieldException {
        List<TypeDescription> fields = orcSchema.getChildren();
        for(TypeDescription field : fields) {
            if(!isSupportedCategory(field.getCategory()))
                throw new OrcConverterUnsupportedFieldException("Unsupported Fields Used in Schema!");
        }
    }

    public class OrcConverterUnsupportedFieldException extends IllegalArgumentException {
        public OrcConverterUnsupportedFieldException() {}

        public OrcConverterUnsupportedFieldException(String message) {
            super(message);
        }
    }
}
