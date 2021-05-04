package top.chenfu.product;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 定时消息生产者
 */
public class ScheduledMessageProducer {

    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("scheduleProducerGroup");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setTopic("scheduleTopic");
            message.setTags("TagA");
            message.setBody(("schedule Message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

//            可在控制台中-集群中查看默认的延迟时间配置，也可以在配置文件中进行配置。总之就是不太灵活
//            且只有开源版的不支持自定义时间|阿里云版支持
//            messageDelayLevel	1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
//            从 1 开始！
            message.setDelayTimeLevel(2);
            producer.send(message);
        }
        producer.shutdown();
    }

}
