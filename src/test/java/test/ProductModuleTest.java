package test;

import custom.client.kafka.Message.Message;
import custom.client.kafka.config.KafkaProducerConfig;
import custom.client.kafka.producer.MyKafkaProducer;

import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @program: kafka-test
 * @description:
 * @author: ZengShiLin
 * @create: 2019-07-09 12:25
 **/
public class ProductModuleTest {

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

    static MyKafkaProducer producer = new MyKafkaProducer(KafkaProducerConfig.builder()
            .bootstrapServers("kafka-service:9092,kafka-service2:9092,kafka-service3:9092")
            .acks("all")
            .enableTransactional(true)
            .enableIdempotence(true)
            .appName("finance-web")
            .build());

    public static void main(String[] args) throws Exception {
        ProductModuleTest test = new ProductModuleTest();
        //KafkaCommonProducer commonProducer = new KafkaCommonProducer();
        Message<String> message = new Message<>(UUID.randomUUID().toString(), "test-topic12", "测试数据【新新1】");
        Message<String> message2 = new Message<>(UUID.randomUUID().toString(), "test-topic12", "测试数据【新新2】");
        //同步发送
        //producer.sendSync(message);


        producer.openTransaction(() -> {
            test.business();
        });


    }


    public void business() {
        //业务环节一发送消息
        Message<String> message = new Message<>(UUID.randomUUID().toString(), "test-topic12", "测试数据【新新1】");
        producer.sendAsync(message);
        //进行其他内容(内部有JDBC事物)
        this.crudByTransaction();
        //业务环节二发送消息
        Message<String> message2 = new Message<>(UUID.randomUUID().toString(), "test-topic12", "测试数据【新新2】");
        producer.sendAsync(message2);
    }


    /**
     * 事物进行
     */
    public void crudByTransaction() {

    }

}
