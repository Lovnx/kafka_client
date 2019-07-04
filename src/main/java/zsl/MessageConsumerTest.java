package zsl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @program: kafka_test
 * @description:
 * @author: ZengShiLin
 * @create: 2019-06-24 18:42
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:*spring-service.xml")
public class MessageConsumerTest {

    /**
     * 为了单一Topic消费，所以一个Topic创建一个
     */
    private boolean isRun = true;

    ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(
            8,
            //CPU密集型任务 核心数量 * 2 + 1（备份线程） maximumPoolSize 不要过多，过多也是进入阻塞队列里面
            9,
            //不等待
            0L,
            TimeUnit.MILLISECONDS,
            //允许等等队列大小 （运维脚本等待时间/每个任务执行的时间 = 任务等待队列长度）
            new LinkedBlockingQueue<>(8)
    );

    @Test
    public void test2() {
        this.receive();
    }

    @Test
    public void receive() {
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(this.properties());
        consumer.subscribe(Collections.singleton("test-topic2"));
        System.out.println("partition信息=" + consumer.partitionsFor("test-topic2"));
        while (isRun) {
            //读取超时时间时间 100ms
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.println("partition=" + records.partitions() + ", offset = " + record.offset() + ", key = " + record.key() + ", value = " + record.value());
            }
            System.out.println("循环,partition=" + records.partitions());
            try {
                consumer.commitAsync();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private Properties properties() {
        Properties properties = new Properties();
        properties.put("acks", "all");
        properties.put("bootstrap.servers", "service1:9092,service2:9092,service3:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        //当设置成1的时候，几乎就算一个一个消息消费了（如果单个消息大于这个值，就返回单条消息）（默认值52428800 大概是 50MB）
        //properties.put("max.partition.fetch.bytes", 1);
        //properties.put("fetch.max.bytes", 1);
        return properties;
    }


}
