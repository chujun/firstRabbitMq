package org.chujun.rabbit.workqueue.v3;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Created by chujun on 16/3/31.
 * one sender,many consumers
 */
public class NewTask {
    public static final String QUEUE_NAME="durable work queues";

    public static int count=1;

    public static String messageMain=":hello world ";
    public static void main(String[] args) throws IOException, TimeoutException {
        System.out.println("start message acknowledgment");
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        //使队列持久化,生产者和消费者队列都需要持久化
        boolean durable=true;
        //RabbitMq不允许定义一个已经存在的队列,却包含不同的参数,它将返回error给企图这么做的程序.
        //这里就有个问题,需要定时去清理一些实际过程中不会用到的队列.(可能因为配置错误,可能因为现在不再使用的队列)
//        Caused by: com.rabbitmq.client.ShutdownSignalException: channel error; protocol method: #method<channel.close>(reply-code=406, reply-text=PRECONDITION_FAILED - inequivalent arg 'durable' for queue 'work queues' in vhost '/': received 'true' but current is 'false', class-id=50, method-id=10)
//        at com.rabbitmq.client.impl.ChannelN.asyncShutdown(ChannelN.java:484)
//        at com.rabbitmq.client.impl.ChannelN.processAsync(ChannelN.java:321)
//        at com.rabbitmq.client.impl.AMQChannel.handleCompleteInboundCommand(AMQChannel.java:144)
//        at com.rabbitmq.client.impl.AMQChannel.handleFrame(AMQChannel.java:91)
//        at com.rabbitmq.client.impl.AMQConnection$MainLoop.run(AMQConnection.java:556)
//        at java.lang.Thread.run(Thread.java:745)

        //At this point we're sure that the task_queue queue won't be lost even if RabbitMQ restarts.
        channel.queueDeclare(QUEUE_NAME,durable,false,false,null);

        int messageCount=10;
        for(int i=0;i<messageCount;i++){
            String message=getMessage(new Date()+messageMain+(count++));
            //非持久化消息
            //channel.basicPublish("",QUEUE_NAME,null,message.getBytes());
            //持久化消息
            channel.basicPublish("",QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes());
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
