# OrcFormat
an OrcFormat implementation to pair with the Confluent kafka-connect-hdfs connector for Kafka Connect

Confluent kafka-connect-hdfs connector: https://github.com/confluentinc/kafka-connect-hdfs

Apache Kafka: https://kafka.apache.org/

Apache ORC: https://orc.apache.org/


# Restrictions
For our purposes we didn't need a reader to read back from ORC to Avro so that was not implemented because of dependency issues between Apache ORC and the orc utility in Apache Hadoop.  ORC is being moved out of hadoop and into it's own package but untill it is fully phased out and the dependency on the old hadoop is changed this implementation didn't work with reading data.  It will still read the schema and that is necessary for testing purposes.
