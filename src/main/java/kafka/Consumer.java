package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class Consumer {
    public static void main(String[] args) {
        String topic="test";
        Properties props = new Properties();
        props.put("bootstrap.servers","dfs01:9092,dfs02:9092,dfs03:9092");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", UUID.randomUUID().toString());
        props.put("auto.offset.reset","earliest");//--from-beginning
        props.put("enable.auto.commit",true);
        props.put("auto.commit.interval.ms", 1000);
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));//订阅消费哪个topic
        while(true){
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for(ConsumerRecord<String ,String> record:consumerRecords){
                System.out.printf("partition=%s;offset=%s;key=%s;value=%s",
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value());
                System.out.println();
            }
        }

    }
}
