package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        String topic="test";
        Properties props = new Properties();
        props.put("bootstrap.servers", "dfs01:9092,dfs03:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("acks","all");
        props.setProperty("retries","2");
        KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(props);
        for(int i=0;i<50;i++){
            producer.send(new ProducerRecord<String, String>(topic,
                    "message".concat(String.valueOf(i))));
        }
        producer.close();

    }
}
