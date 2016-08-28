package com.kalpada.jms;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.*;

/**
 * Created by Kalpa on 27/8/16.
 */
public class Consumer {//implements MessageListener {

    private static ConnectionFactory factory = null;
    private static Connection connection = null;
    private Session session = null;
    private Destination destination = null;
    private MessageConsumer consumer = null;

    static {
        try {
            factory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_BROKER_URL);
            connection = factory.createConnection();
            connection.start();
        } catch (JMSException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public Consumer() {
        initSession();
    }

    public void initSession() {
        try {

            //session = connection.createSession(false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
            session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            destination = session.createQueue("home.jms.activemq_q1");
            consumer = session.createConsumer(destination);
            //consumer.setMessageListener(this);
            //consumer.receive();
        } catch (JMSException ex) {
            ex.printStackTrace();
        }
    }

    public void handleMessage() {
        try {
            System.out.println("Polling for new message..");
            Message message = consumer.receive(5);

            TextMessage text = null;
            if (message != null) {
                System.out.print("Messsage Redelivered Status: " + message.getJMSRedelivered());
                if (message instanceof TextMessage) {
                    text = (TextMessage) message;
                    System.out.println("Message is : " + text.getText());
                }
                /*if (text != null && text.getText().equals("Message :1")) {
                    throw new JMSException("ex");
                }*/

                System.out.println("Ack the message..");
                message.acknowledge();
            } else {
                System.out.println("No message received, skip processing.. ");
            }

        } catch (JMSException e) {
            System.out.println("Exception detected..");
            e.printStackTrace();
        } finally {
            System.out.println("Closing connection");
            if (consumer != null) {
                try {
                    consumer.close();
                    consumer = null;
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (session != null) {
                try {
                    session.close();
                    session = null;
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
           /* if(connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }

    public void onMessage(Message message) {
        try {
            System.out.println("Receive new message..");
            if (message instanceof TextMessage) {

                TextMessage text = (TextMessage) message;
                System.out.println("Message is : " + text.getText());

                if (text.getText().equals("Message :1")) {
                    throw new JMSException("ex");
                }
            }

            System.out.print("Ack the message..");
            //session.commit();
            //acknowledge that all messages have been received
            message.acknowledge();

        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println("Waiting for newt message..");
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
    }
}
