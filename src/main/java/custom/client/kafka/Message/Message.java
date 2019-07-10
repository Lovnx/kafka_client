package custom.client.kafka.Message;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * @program: kafka-test
 * @description: kafka 消息体
 * @author: ZengShiLin
 * @create: 2019-07-09 12:36
 **/
@Builder
@Accessors(chain = true)
public class Message<T> {

    /**
     * 需要发送的topic(创建后就不允许修改)
     */
    @Getter
    private final String topic;

    /**
     * 消息的key值(创建后就不允许修改)
     */
    @Getter
    private final String key;

    /**
     * 消息的value值(创建后就不允许修改)
     */
    @Getter
    private final T value;


    public Message(String key, String topic, T value) {
        this.key = key;
        this.value = value;
        this.topic = topic;
    }

}
