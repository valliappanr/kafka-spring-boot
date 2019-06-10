package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaConsumerClient {
    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        String bootStrapServer = System.getenv().get("BOOTSTRAP_SERVER");
        String topic = System.getenv().get("KAFKA_TOPIC");;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        boolean flag = true;

        while (true) {
            if (flag) {
                List<TopicPartition> partitions =consumer.partitionsFor("users").stream().map(part->{TopicPartition tp = new TopicPartition(part.topic(),part.partition()); return tp;}).collect(Collectors.toList());
                Field coordinatorField = consumer.getClass().getDeclaredField("coordinator");
                coordinatorField.setAccessible(true);

                ConsumerCoordinator coordinator = (ConsumerCoordinator)coordinatorField.get(consumer);
                coordinator.poll(new Date().getTime(), 1000);//Watch out for your local date and time settings
                partitions.stream().forEach(partition -> {
                    consumer.seek(partition, 10);
                });
                flag = false;
            }

            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

}
