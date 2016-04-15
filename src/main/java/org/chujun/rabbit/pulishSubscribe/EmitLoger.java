package org.chujun.rabbit.pulishSubscribe;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Created by chujun on 16/4/15.
 * 第三个程序
 * 日志记录,
 * 一个日志生成器,多个日志接受器
 * binding:表示exchange和queue之间的关系.queue对来自exchange的消息感兴趣.
 *
 * 广播消息到所有接受者
 *
 * RabbitMq消息模型
 * 1.生产者:产生消息的用户程序.
 * 2.队列:存储消息的缓存器.
 * 3.消费者:接受消息的用户程序.
 *
 * 消息模型核心概念:生产者不直接发送消息给队列.甚至生产者根本不知道消息是否分发给队列.
 * 那生产者讲消息发送给谁?
 * 实际上生产者仅发送消息给exchange.
 * exchange:一方面接受来自生产者的消息,另一方面将消息推送给队列.
 * exchange必须知道接受来的消息下一步怎么办?发送给特定队列?发送给多个队列?还是被抛弃?
 * 这个规则室友exchange的类型决定的.
 *
 * exchange类型包括
 * a.direct
 * b.topic
 * c.headers
 * d.fanout(扇形)
 *
 * 这个程序讨论fanout类型的exchange:广播它接受到的消息到所有它知道的队列.
 *
 * 有个疑问之前的程序没有exchange,为什么也可以发送消息成功呢?
 * $ sudo rabbitmqctl list_exchanges
 * 查看所有的exchange,存在一个未命名的exchange(rabbitMq默认创建)
 * 前面的程序使用空字符串的exchange,意味着使用默认的exchange,消息将被路由到名称为routingKey
 * 指定的队列,如果routingKey存在的话.
 *
 *
 * 临时队列
 * 需求1:无论何时连接RabbitMq,都需要一个新生的空队列.为了做到这一点,我们可以创建一个随机名的队列.
 * 更好的方式:让RabbitMq为我们选择随机的队列名.
 * 需求2:一旦我们断开消费者,队列自动删除.
 * 如下代码实现:
 * String queueName = channel.queueDeclare().getQueue();
 */
public class EmitLoger {

    public static final String EXCHANGE_NAME="logs";

    public static final String EXCHANGE_TYPE="fanout";

    public static int count=1;

    public static String messageMain=":hello world ";
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
        //无需再申明queue
        int messageCount=10000;
        for(int i=0;i<messageCount;i++){
            String message=getMessage(new Date()+messageMain+(count++));
            channel.basicPublish(EXCHANGE_NAME,"",null,message.getBytes());
            System.out.println("send :'"+message+"'");
        }
        channel.close();
        connection.close();
    }

    public static String getMessage(String message){
        String default_message=messageMain;
        if(null==message){
            message=new Date()+default_message+(count++);
        }
        int count=generateRandomCount(10);
        for(int i=0;i<count;i++){
            message+=".";
        }
        return message;
    }

    /**
     *产生1到baseNum的数
     * @param baseNum
     * @return
     */
    public static int generateRandomCount(Integer baseNum){
        int num=10;
        if(null!=baseNum&&baseNum>0){
            num=baseNum;
        }
        int count=(int)(1+Math.random()*baseNum);
        System.out.println("current count:"+count);
        return count;
    }
}
