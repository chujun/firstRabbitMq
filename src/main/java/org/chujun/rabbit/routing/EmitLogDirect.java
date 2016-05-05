package org.chujun.rabbit.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Created by chujun on 16/5/5.
 */
public class EmitLogDirect {
    public static final String EXCHANGE_NAME="logs-severity";

    public static final String EXCHANGE_TYPE="direct";

    public static final String VIRTUAL_HOST="log";

    public static int count=1;

    public static String messageMain=":hello world ";
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setVirtualHost(VIRTUAL_HOST);
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
        //无需申明queue
        int messageCount=10000;
        for(int i=0;i<messageCount;i++){
            String message=getMessage(new Date()+messageMain+(count++));
            String severity = getSeverity(i);
            channel.basicPublish(EXCHANGE_NAME,severity,null,message.getBytes());
            System.out.println("send ["+severity+"]:'"+message+"'");
        }
        //channel.close();
        //connection.close();
    }

    public static String getSeverity(int random){
        String[] severitys=new String[]{"debug","info","warn","error"};
        return severitys[random%4];
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
