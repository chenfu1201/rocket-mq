package top.chenfu.product;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FilterProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("filterProducerGroup");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        List<String> tagList = Arrays.asList("TagA", "TagB", "TagC");
        for (int i = 0; i < 8; i++) {
            Message message = new Message();
            message.setTopic("filterTopic");
            message.setTags(tagList.get(i % tagList.size()));
            Map<String, String> properties = message.getProperties();
            properties.put("c1", String.valueOf(i));
            message.putUserProperty("c2", "v" + i);
            message.setBody(("fitler message Body " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

            SendResult send = producer.send(message);
            System.out.println(send);
        }
        producer.shutdown();
    }
}
