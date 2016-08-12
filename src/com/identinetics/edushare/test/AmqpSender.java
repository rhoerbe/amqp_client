package com.identinetics.edushare.test;

import java.io.*;
import java.util.concurrent.TimeoutException;

/**
 * Basic aqmp sender
 * Includes simple blocking receiver for test purposes
 */
import com.rabbitmq.client.*;

public class AmqpSender {
    Channel channel = null;
    Connection conn = null;
    String q = null;

    public AmqpSender(String queueName) throws IOException, TimeoutException {
        q = queueName;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("amqp.test.portalverbund.at");
        factory.setPort(5672);
        factory.setUsername("rabbi09");
        factory.setPassword("changemeforprod");
        factory.setVirtualHost("/");
        //factory.useSslProtocol();

        conn = factory.newConnection();
        channel = conn.createChannel();

        //non-durable, exclusive, auto-delete queue (change to durable queue for production)
        channel.queueDeclare(q, true, true, true, null);
    }

    void send(String message) throws IOException, TimeoutException {
        channel.basicPublish("", q, null, message.getBytes());

    }

    String receive() throws IOException, TimeoutException {
        GetResponse chResponse = channel.basicGet(q, false);
        if (chResponse == null) {
            return(null);
        } else {
            byte[] body = chResponse.getBody();
            return(new String(body));
        }
    }

    void close() throws IOException, TimeoutException {
        channel.close();
        conn.close();
    }

    public static void main(String[] args) throws Exception {
        AmqpSender s = new AmqpSender("sokrates_updates_test3");
        s.send("Hallo Welt");
        System.out.println(s.receive());
        s.close();

    }
}