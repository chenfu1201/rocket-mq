package top.chenfu.product;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import top.chenfu.domain.OrderStep;

import java.util.Arrays;
import java.util.List;

/**
 * 顺序消息生产者
 */
public class OrderProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("orderProducerGroup");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

//        Tag 类型
        List<String> tagList = Arrays.asList("TagA", "TagB", "TagC");
//        订单列表
        List<OrderStep> orderList = OrderStep.buildOrders();

        for (int i = 0; i < orderList.size(); i++) {
            Message message = new Message();
            message.setTopic("orderTopic");
            message.setTags(tagList.get(i % tagList.size()));
            message.setBody(("OrderProducer message " + orderList.get(i)).getBytes(RemotingHelper.DEFAULT_CHARSET));

            SendResult send = producer.send(message, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    long orderId = Long.parseLong(arg.toString());
                    return mqs.get((int) (orderId % mqs.size()));
                }
            }, orderList.get(i).getOrderId());

            System.out.println(String.format("SendResult status:%s, queueId:%d, body:%s",
                    send.getSendStatus(),
                    send.getMessageQueue().getQueueId(),
                    new String(message.getBody())));
        }
        producer.shutdown();
    }
}
