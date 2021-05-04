package top.chenfu.product;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class OneWayProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setProducerGroup("oneWayProducerGroup");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setTopic("oneWayTopic");
            message.setTags("TagC");
            message.setBody(("OneWayProducer message-" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

            System.out.println("send - " + message);
            producer.sendOneway(message);
        }

        producer.shutdown();
    }
}
