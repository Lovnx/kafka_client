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
    PRODUCER_POOL_INITIALIZE_FAILURE(10001, "生产者池初始化失败"),
    PRODUCER_APPNAME_IS_EMPTY(10002, "生产者APP名称为空"),
    PRODUCER_SEND_FAILURE(10003, "生产者发送消息失败");


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
