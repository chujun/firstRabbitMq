package org.chujun.rabbit.topics;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Created by chujun on 16/5/5.
 */
public class ReceiveLogsForStarCharTopic {
    private static final String USER_NAME = "study";

    private static final String PASSWORD = "study";

    public static final String EXCHANGE_NAME="logs-severity-source";

    public static final String EXCHANGE_TYPE="topic";

    public static final String VIRTUAL_HOST="log-topic";

    public static final String[] SEVERITYS=new String[]{"debug","info","warn","error"};

    public static final String[] SOURCES=new String[]{"kern","normal","program"};


    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername(USER_NAME);
        connectionFactory.setPassword(PASSWORD);
        connectionFactory.setVirtualHost(VIRTUAL_HOST);
        Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();

        //exchange
        channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
        //random queue
        String queueName = channel.queueDeclare().getQueue();
        //形如amq.gen-w61kUie5tX4BvlREFcXCig
        System.out.println("random queueName:"+queueName);

        //bind
        //Subscribing
        setBindingKey(channel,queueName);


        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer=new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag,Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException{
                String message=new String(body,"UTF-8");
                try {
                    System.out.println("[x] Received "+envelope.getRoutingKey()+":'"+message+"'");
                    doWork(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }finally {
                    System.out.println("[x]"+new Date()+": done '"+message+"'\n");
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }
            }
        };
        channel.basicConsume(queueName,false,consumer);
    }

    private static void setBindingKey(Channel channel,String queueName) throws IOException {
        channel.queueBind(queueName,EXCHANGE_NAME,SOURCES[1]+".*");
    }

    /**
     * 通过解析消息中的"."模拟处理消息的耗时秒数
     * @param message
     */
    public static void doWork(String message) throws InterruptedException {
        for(char ch:message.toCharArray()){
            if(ch=='.'){
                Thread.sleep(50);
            }
        }
    }
}
