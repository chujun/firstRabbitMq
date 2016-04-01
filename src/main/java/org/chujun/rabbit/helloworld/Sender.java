package org.chujun.rabbit.helloworld;

/**
 * Created by chujun on 16/3/30.
 */
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

public class Sender {
    public static final String QUEUE_NAME="hello world";
    public static  void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("localhost");
        //The connection abstracts the socket connection, and takes care of protocol version negotiation and authentication and so on for us.
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        String message="hello world:"+new Date();
        channel.basicPublish("",QUEUE_NAME,null,message.getBytes());
        System.out.println("[x] sent '"+message+"'");

        channel.close();
        connection.close();
    }
}
