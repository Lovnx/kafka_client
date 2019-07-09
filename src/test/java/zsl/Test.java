package zsl;

import custom.client.kafka.producer.MyKafkaProducer;

/**
 * @program: kafka-test
 * @description:
 * @author: ZengShiLin
 * @create: 2019-07-09 12:25
 **/
public class Test {

    public static void main(String[] args) {
        MyKafkaProducer producer = new MyKafkaProducer();

        /**
         * 开启事务
         */
        producer.sendByTransaction(() -> {

        });

    }
}
