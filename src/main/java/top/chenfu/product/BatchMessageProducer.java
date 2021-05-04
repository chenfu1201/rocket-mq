package top.chenfu.product;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * 批量消息
 */
public class BatchMessageProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("batchProducerGroup");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        String topic = "batchTopic";
        List<String> tagList = Arrays.asList("TagA", "TagB", "TagC");

        for (int i = 0; i < 2; i++) {
            LinkedList<Message> messageList = new LinkedList<Message>();
            for (int j = 0; j < 5; j++) {
                Message message = new Message();
                message.setTopic(topic);
                message.setTags(tagList.get(j % tagList.size()));
                message.setBody(String.format("batch Message %s %s ", i, j).getBytes(RemotingHelper.DEFAULT_CHARSET));
                messageList.add(message);
            }
            SendResult send = producer.send(messageList);
            System.out.println(send.getMessageQueue());
        }
        producer.shutdown();
    }

}
