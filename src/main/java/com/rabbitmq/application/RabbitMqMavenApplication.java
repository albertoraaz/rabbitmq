package com.rabbitmq.application;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import java.io.IOException;
import org.bson.Document;
import com.mongodb.client.MongoCollection;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

/**
 * The Class RabbitMqMavenApplication.
 */
@SpringBootApplication
public class RabbitMqMavenApplication {

	/** The Constant QUEUE_NAME. */
	private final static String QUEUE_NAME = "test-queue";

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		try {

			MongoClientURI uri = new MongoClientURI(
					"mongodb+srv://araaz:Ven320wl8ven!@cluster0-6qzob.gcp.mongodb.net/test?retryWrites=true&w=majority");

			MongoClient mongoClient = new MongoClient(uri);
			MongoDatabase database = mongoClient.getDatabase("test");

			System.out.println("MONGODB CONNECTED XXXXXX" + database.getName());

			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			factory.setUsername("admin");
			factory.setPassword("admin");
			factory.setVirtualHost("/");

			com.rabbitmq.client.Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			try {
				channel.queueDeclare(QUEUE_NAME, true, false, false, null);
				System.out.println(" [*] Waiting for messages.");

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
