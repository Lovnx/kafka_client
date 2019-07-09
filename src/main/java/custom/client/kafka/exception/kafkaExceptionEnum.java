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
    PRODUCER_CONFIGURATION_IS_EMPTY(10000, "生产者配置为空");

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
