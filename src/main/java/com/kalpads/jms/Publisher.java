package com.kalpads.jms;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Created by Kalpa on 27/8/16.
 */
public class Publisher {

    public void sendMessage(String messageContent) {

        ConnectionFactory factory = null;
        Connection connection = null;
        Session session = null;
        Destination destination = null;
        MessageProducer producer = null;

        try {

            factory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_BROKER_URL);
            connection = factory.createConnection();
            connection.start();

            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue("home.jms.activemq_q1");
            producer = session.createProducer(destination);
            TextMessage message = session.createTextMessage();
            message.setText(messageContent);
            producer.send(message);
            System.out.println("Sent: " + message.getText());


        } catch (JMSException e) {
            e.printStackTrace();
        } finally {

            if(producer != null) {
                try {
                    producer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if(session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if(connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String [] args) {
        Publisher publisher = new Publisher();
        for(int i= 0;i<3;i++) {
            publisher.sendMessage("Message :"+ i);
        }
    }
}
