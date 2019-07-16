package test;

import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * @program: kafka_test
 * @description:
 * @author: ZengShiLin
 * @create: 2019-06-24 18:42
 **/
public class MessageConsumerTest {


    public static void main(String[] args) {
        MessageConsumerTest test = new MessageConsumerTest();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(test.properties());
        consumer.subscribe(Arrays.asList("test-topic7", "test-topic8", "test-topic12", "test-topice12", "test-test-topic12"));
        System.out.println("partition信息=" + consumer.partitionsFor("test-topic12"));
        Map<TopicPartition, OffsetAndMetadata> metadataMap = Maps.newHashMap();
        while (true) {
            //读取超时时间 100ms
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic:" + record.topic() + ",partition=" + records.partitions() + ", offset = " + record.offset() + ", key = " + record.key() + ", value = " + record.value());
                //手动提交  offset + 1，下次消费者从该偏移量开始拉取消息 (metadata 提交的一些额外信息)
                metadataMap.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no"));
            }
            System.out.println("循环,partition=" + records.partitions());
            try {
                //手动提交 同步
                consumer.commitSync(metadataMap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private Properties properties() {
        Properties properties = new Properties();
        properties.put("acks", "all");
        //properties.put("bootstrap.servers", "service1:9092,service2:9092,service3:9092");
        properties.put("bootstrap.servers", "kafka-service:9092,kafka-service2:9092,kafka-service3:9092");
        //properties.put("bootstrap.servers", "kafka-0.kafka-svc.docker36.svc.cluster.local:9092,kafka-1.kafka-svc.docker36.svc.cluster.local:9092,kafka-2.kafka-svc.docker36.svc.cluster.local:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //消费者会自动Rebalance
        properties.put("group.id", "test3");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "latest");
        //properties.put("auto.commit.interval.ms", "1000");
        //properties.put("session.timeout.ms", "30000");
        //当设置成1的时候，几乎就算一个一个消息消费了（如果单个消息大于这个值，就返回单条消息）（默认值52428800 大概是 50MB）
        //properties.put("max.partition.fetch.bytes", 1);
        //properties.put("fetch.max.bytes", 1);
        return properties;
    }


}
