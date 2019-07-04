package zsl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Create by ZengShiLin on 2019-06-30
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:*spring-service.xml")
public class MessageProductTraditionTest {

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
    public void test() throws InterruptedException {
        poolExecutor.execute(this::producer);
        poolExecutor.execute(this::producer2);
        Thread.sleep(3000);
    }


    public void producer() {
        try {
            KafkaProducer<Integer, String> producer = new KafkaProducer<>(this.properties());
            producer.initTransactions();
            producer.beginTransaction();
            for (int index = 0; index < 5; index++) {
                ProducerRecord<Integer, String> record = new ProducerRecord<>("test-topic", 123, "测试数据2-" + index);
                Future future = producer.send(record);
                future.get();
                System.out.println("发送");
                Thread.sleep(500);
            }
            producer.flush();
            producer.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void producer2() {
        try {
            KafkaProducer<Integer, String> producer2 = new KafkaProducer<>(this.properties2());
            producer2.initTransactions();
            producer2.beginTransaction();
            for (int index = 0; index < 5; index++) {
                ProducerRecord<Integer, String> record = new ProducerRecord<>("test-topic2", 123, "测试数据2-" + index);
                Future future = producer2.send(record);
                future.get();
                System.out.println("发送");
            }
            producer2.flush();
            producer2.abortTransaction();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private Properties properties() {
        Properties properties = new Properties();
        properties.put("acks", "all");
        properties.put("bootstrap.servers", "service1:9092,service2:9092,service3:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("enable.idempotence", true);
        properties.put("transactional.id", "test_transactional.id");
        properties.put("client.id", "ProducerTranscationnalExample");
        return properties;
    }


    private Properties properties2() {
        Properties properties = new Properties();
        properties.put("acks", "all");
        properties.put("bootstrap.servers", "service1:9092,service2:9092,service3:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("enable.idempotence", true);
        properties.put("transactional.id", "test_transactional.id2");
        properties.put("client.id", "ProducerTranscationnalExample2");
        return properties;
    }
}
