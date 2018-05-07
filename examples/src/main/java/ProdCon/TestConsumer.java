package ProdCon;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class TestConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestConsumer.class);
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> testCon = new KafkaConsumer<String, String>(props);
        testCon.subscribe(Arrays.asList("Topic3"));
        try{
            while(true){
                ConsumerRecords<String, String> records = testCon.poll(100);
                for(ConsumerRecord<String, String> record : records){
                    System.out.println(record.toString());
                }
            }
        } catch(Exception e){
            LOGGER.error("Exception caught while consuming", e);
        }finally{
            testCon.close();
        }
    }
}
