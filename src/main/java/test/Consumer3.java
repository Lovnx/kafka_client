package test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Create by ZengShiLin on 2019-07-06
 */
public class Consumer3 {


    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "service1:9092,service2:9092,service3:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", false);
        //自动提交时间
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("test-topice10"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(5000);
            System.out.println("循环拉取数据：");
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("消息内容：" + record.value());
            }
        }

    }
}
