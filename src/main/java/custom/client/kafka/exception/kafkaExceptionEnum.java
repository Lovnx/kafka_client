package custom.client.kafka.exception;

import lombok.Getter;

/**
 * kafka 异常枚举
 *
 * @author ZengShiLin
 */

public enum kafkaExceptionEnum {

    /**
     * 异常错误码和异常内容
     */
    PRODUCER_CONFIGURATION_IS_EMPTY(10000, "生产者配置为空"),
    PRODUCER_THREADLOCAL_INITIALIZE_FAILURE(10001, "ThreadLocal生产者初始化失败"),
    PRODUCER_POOL_INITIALIZE_FAILURE(10002, "生产者对象池初始化失败"),
    PRODUCER_APPNAME_IS_EMPTY(10003, "生产者APP名称为空"),
    PRODUCER_SEND_FAILURE(10004, "生产者发送消息失败"),
    CONSUMER_CONFIGURATION_IS_EMPTY(10005, "消费者配置为空");

    /**
     * 枚举value
     */
    @Getter
    private int value;
    /**
     * 枚举名称
     */
    @Getter
    private String name;

    kafkaExceptionEnum(int value, String name) {
        this.value = value;
        this.name = name;
    }

}
