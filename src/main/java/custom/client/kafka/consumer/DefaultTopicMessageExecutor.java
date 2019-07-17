package custom.client.kafka.consumer;

import java.util.UUID;

/**
 * @program: kafka-test
 * @description: 默认实现的消费者实例配置
 * @author: ZengShiLin
 * @create: 2019-07-17 14:17
 **/
public abstract class DefaultTopicMessageExecutor implements TopicMessageExecutor {

    @Override
    public String getUniqueName() {
        return this.getTopic() + "_" + UUID.randomUUID().toString();
    }


    @Override
    public boolean needManualCommit() {
        return true;
    }

    @Override
    public int commitThreshold() {
        return 1;
    }
}
