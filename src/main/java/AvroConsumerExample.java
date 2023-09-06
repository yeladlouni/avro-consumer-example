import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import kafka.Kafka;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroConsumerExample {
    public AvroConsumerExample() {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-avro");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);

        String topic = "customers";

        consumer.subscribe(Collections.singleton(topic));

        while (true) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, GenericRecord> record: records) {
                String key = record.key();
                GenericRecord customer = record.value();

                System.out.printf("key=%s, id=%d, first_name=%s, last_name=%s, age=%d, gender=%s, salary=%.2f\n",
                        key,
                        customer.get("id"),
                        customer.get("first_name"),
                        customer.get("last_name"),
                        customer.get("age"),
                        customer.get("gender"),
                        customer.get("salary")
                );
            }
        }
    }

    public static void main(String[] args) {
        new AvroConsumerExample();
    }
}
