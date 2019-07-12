package zsl;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Create by ZengShiLin on 2019-06-30
 */
public class MessageProductTraditionTest {

    static ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(
            8,
            //CPU密集型任务 核心数量 * 2 + 1（备份线程） maximumPoolSize 不要过多，过多也是进入阻塞队列里面
            9,
            //不等待
            0L,
            TimeUnit.MILLISECONDS,
            //允许等等队列大小 （运维脚本等待时间/每个任务执行的时间 = 任务等待队列长度）
            new LinkedBlockingQueue<>(8)
    );

    public static void main(String[] args) {
        MessageProductTraditionTest test = new MessageProductTraditionTest();
        //poolExecutor.execute(test::testProduceLocal);
        poolExecutor.execute(test::testProduceLocal2);
        //MessageProductTraditionTest.testProduce2();
        //test.testProduce();
    }

    static ThreadLocal<KafkaProducer<String, String>> producerThreadLocal = ThreadLocal.withInitial(() -> new KafkaProducer<>(MessageProductTraditionTest.properties3()));

    /**
     * 测试线程事务隔离性
     */
    public void testProduceLocal() {
        try {
            producerThreadLocal.get().initTransactions();
            producerThreadLocal.get().beginTransaction();
            Thread.sleep(2000);
            for (int index = 0; index < 5; index++) {
                ProducerRecord<String, String> record2 = new ProducerRecord<>("test-topic12", UUID.randomUUID().toString(), "线程一测试数据-test-topic12" + index);
                producerThreadLocal.get().send(record2).get();
                System.out.println("发送2");
            }
            //回滚
            producerThreadLocal.get().abortTransaction();
        } catch (Exception e) {
            System.out.println("事务回滚" + e.getMessage());
            //回滚
            producerThreadLocal.get().abortTransaction();
        }
    }


    /**
     * 测试线程事务隔离性
     */
    public void testProduceLocal2() {
        try {
            //producerThreadLocal.get().initTransactions();
            //producerThreadLocal.get().beginTransaction();
            for (int index = 0; index < 5; index++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("test-topic12", UUID.randomUUID().toString(), "test-topice12测试事务数据666-" + index);
                //ProducerRecord<String, String> record2 = new ProducerRecord<>("test-topic8", UUID.randomUUID().toString(), "线程二测试数据3-test-topic8" + index);
                Future<RecordMetadata> future = producerThreadLocal.get().send(record);
//                if (true) {
//                    throw new RuntimeException("测试异常");
//                }
                //producerThreadLocal.get().send(record2);
                RecordMetadata metadata = future.get();
                System.out.println("发送成功,metadata" + JSON.toJSONString(metadata));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("事务回滚");
            //回滚
            //producerThreadLocal.get().abortTransaction();
        }
    }


    /**
     * 测试线程事务隔离性
     */
    public void testProduce() {
        MessageProductTraditionTest test = new MessageProductTraditionTest();
        KafkaProducer<String, String> producer2 = new KafkaProducer<>(test.properties2());
        try {
            producer2.initTransactions();
            producer2.beginTransaction();
            Thread.sleep(2000);
            for (int index = 0; index < 5; index++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("test-topic7", UUID.randomUUID().toString(), "线程一测试事务数据666-" + index);
                ProducerRecord<String, String> record2 = new ProducerRecord<>("test-topic8", UUID.randomUUID().toString(), "线程一测试数据3-test-topic8" + index);
                System.out.println("发送1");
                Future future = producer2.send(record);
//                if (true) {
//                    throw new RuntimeException("测试异常");
//                }
                producer2.send(record2);
                future.get();
                System.out.println("发送2");
            }
        } catch (Exception e) {
            System.out.println("事务回滚");
            //回滚
            producer2.abortTransaction();
        }
    }


    /**
     * 测试线程事务隔离性
     */
    public static void testProduce2() {
        MessageProductTraditionTest test = new MessageProductTraditionTest();
        KafkaProducer<String, String> producer2 = new KafkaProducer<>(test.properties2());
        try {
            producer2.initTransactions();
            producer2.beginTransaction();
            Thread.sleep(2000);
            for (int index = 0; index < 5; index++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("test-topic7", UUID.randomUUID().toString(), "线程二测试事务数据666-" + index);
                ProducerRecord<String, String> record2 = new ProducerRecord<>("test-topic8", UUID.randomUUID().toString(), "线程二测试数据3-test-topic8" + index);
                System.out.println("发送1");
                Future future = producer2.send(record);
//                if (true) {
//                    throw new RuntimeException("测试异常");
//                }
                producer2.send(record2);
                future.get();
                System.out.println("发送2");
            }
        } catch (Exception e) {
            System.out.println("事务回滚");
            //回滚
            producer2.abortTransaction();
        }
    }


    @Test
    public void producer() {
        try {
            KafkaProducer<String, String> producer = new KafkaProducer<>(this.properties());

            for (int index = 0; index < 5; index++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("test-topic7", "123", "测试数据3-test-topic7" + index);
                Future future = producer.send(record);
                future.get();
                System.out.println("发送");
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private Properties properties() {
        Properties properties = new Properties();
        properties.put("acks", "all");
        properties.put("bootstrap.servers", "service1:9092,service2:9092,service3:9092");
        //properties.put("bootstrap.servers", "kafka-service:9092,kafka-service2:9092,kafka-service3:9092");
        //properties.put("bootstrap.servers", "kafka-0.kafka-svc.docker36.svc.cluster.local:9092,kafka-1.kafka-svc.docker36.svc.cluster.local:9092,kafka-2.kafka-svc.docker36.svc.cluster.local:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("enable.idempotence", true);
        //properties.put("transactional.id", "test_transactional.id");
        properties.put("client.id", "ProducerTranscationnalExample");
        return properties;
    }


    private static Properties properties2() {
        Properties properties = new Properties();
        properties.put("acks", "all");
        properties.put("bootstrap.servers", "kafka-0.kafka-svc.docker36.svc.cluster.local:9092,kafka-1.kafka-svc.docker36.svc.cluster.local:9092,kafka-2.kafka-svc.docker36.svc.cluster.local:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("enable.idempotence", true);
        properties.put("transactional.id", UUID.randomUUID().toString());
        properties.put("client.id", "ProducerTranscationnalExample2");
        properties.put("isolation.level", "read_committed");
        return properties;
    }


    public static Properties properties3() {
        Properties properties = new Properties();
        properties.put("acks", "all");
        properties.put("bootstrap.servers", "kafka-service:9092,kafka-service2:9092,kafka-service3:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("enable.idempotence", true);
        properties.put("client.id", "ProducerTranscationnalExample2");
        return properties;
    }


    public static Properties properties4() {
        Properties properties = new Properties();
        properties.put("acks", "all");
        properties.put("bootstrap.servers", "kafka-service:9092,kafka-service2:9092,kafka-service3:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("enable.idempotence", true);
        properties.put("transactional.id", UUID.randomUUID().toString());
        properties.put("client.id", "ProducerTranscationnalExample2");
        properties.put("isolation.level", "read_committed");
        return properties;
    }
}
