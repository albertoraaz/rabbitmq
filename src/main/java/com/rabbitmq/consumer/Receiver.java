package com.rabbitmq.consumer;

import java.io.IOException;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class Receiver {

	private final static String QUEUE_NAME = "test-queue";

	public static void main(String[] argv) throws Exception {

		try {

			ConnectionString connString = new ConnectionString(
					"mongodb+srv://araaz:Ven320wl8ven!@cluster0-6qzob.gcp.mongodb.net/test?retryWrites=true&w=majority");

			MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connString)
					.retryWrites(true).build();

			MongoClient mongoClient = MongoClients.create(settings);

			MongoDatabase database = mongoClient.getDatabase("test");

			System.out.println("MongoDB connected to: " + database.getName());

			try {
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost("localhost");
				factory.setUsername("admin");
				factory.setPassword("admin");
				Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();
				channel.queueDeclare(QUEUE_NAME, true, false, false, null);

				Consumer consumer = new DefaultConsumer(channel) {
					@Override
					public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
							byte[] body) throws IOException {
						String message = new String(body, "UTF-8");
						System.out.println(String.format("Received  «%s»", message));

						Document doc = Document.parse(message.toString());
						MongoCollection<Document> collection = database.getCollection("testCollection");
						collection.insertOne(doc);
					}
				};

				channel.basicConsume(QUEUE_NAME, true, consumer);

				Thread.sleep(20000);

			} catch (Exception ex) {
				ex.printStackTrace();
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}