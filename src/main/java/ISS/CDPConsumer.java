package ISS;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Properties;

/**
 * chronic-disease-project 慢病项目的consumer
 * Created by zhangbo on 2018/4/16
 */
public class CDPConsumer {
    public static void main(String[] args) throws UnknownHostException{
        Properties props = new Properties();
        //props.put("zookeeper.connect", "192.168.222.5:2181");
        props.put("bootstrap.servers", "192.168.222.226:9092");
        props.put("group.id", "a");
        props.put("schema.registry.url", "http://192.168.222.226:8081");
        props.put("enable.auto.commit", false);
        props.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
        //props.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put("value.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put("auto.offset.reset", "earliest");
        System.out.println("running!");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        kafkaConsumer.subscribe(Arrays.asList("blood-pressure", "body-temperature", "body-fat-percentage","heart-rate",
                "step-count","sleep-duration"));
        while(true){
            ConsumerRecords records = kafkaConsumer.poll(1000);

            for(Object record : records) {
                ConsumerRecord consumerRecord = ((ConsumerRecord) record);
                System.out.printf("the key of the record is %s, and the value is %s, the partition of the record is %s, offset is %s \n, ",
                        consumerRecord.key(),
                        consumerRecord.value(),
                        consumerRecord.partition(),
                        consumerRecord.offset());
            }
        }
    }
}