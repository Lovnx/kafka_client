package test;

import custom.client.kafka.Message.Message;
import custom.client.kafka.producer.KafkaCommonProducer;

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
public class Test {

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

    public static void main(String[] args) throws Exception {
        KafkaCommonProducer commonProducer = new KafkaCommonProducer();
        //同步发送
        commonProducer.sendSync(Message.builder()
                .topic("test-topic12")
                .key(UUID.randomUUID().toString())
                .value("测试数据")
                .build());
    }
}
