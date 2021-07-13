package com.example.kafkaandspringboot.message.consumer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import com.example.kafkaandspringboot.dto.Greeting;

public class MessageConsumer {
	
	private Logger logger = LoggerFactory.getLogger(MessageConsumer.class);
	
	public CountDownLatch latch = new CountDownLatch(3);
	
	public CountDownLatch partitionLatch = new CountDownLatch(2);

	public CountDownLatch filterLatch = new CountDownLatch(2);

	public CountDownLatch greetingLatch = new CountDownLatch(1);
	
	/*For a topic with multiple partitions, however, a @KafkaListener can explicitly subscribe 
	 * to a particular partition of a topic with an initial offset. Since the initialOffset has 
	 * been set to 0 in this listener, all the previously consumed messages from partitions 0 and 3 
	 * will be re-consumed every time this listener is initialized.*/
	@KafkaListener(
			topicPartitions = @TopicPartition(topic = "${partitioned.topic.name}", 
			partitionOffsets = {
					@PartitionOffset(partition = "0", initialOffset = "0"),
					@PartitionOffset(partition = "3", initialOffset = "0")}),
			containerFactory = "partitionsKafkaListenerContainerFactory")
	public void listenToPartition(@Payload String message, 
								  @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		logger.info("Received message 'To Partition': " + message + "from partition: " + partition);
		partitionLatch.countDown();
	}
	
	/*If we don't need to set the offset, we can use the partitions property of @TopicPartition annotation 
	 * to set only the partitions without the offset*/
//	@KafkaListener(
//			topicPartitions = @TopicPartition(topic = "${partitioned.topic.name}", 
//			partitions = { "0", "1" }), 
//			containerFactory = "partitionsKafkaListenerContainerFactory")
//	public void listenToPartition2(@Payload String message, 
//								   @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
//		logger.info("Listen to Partition2. Received message: " + message + "from partition: " + partition);
//		partitionLatch.countDown();
//	}
	
	/*@KafkaListener(topics = "topic1, topic2", groupId = "foo") //one consumer can listen for messages from various topics.*/
	@KafkaListener(topics = "${message.topic.name}", groupId = "foo", containerFactory = "fooKafkaListenerContainerFactory")
	public void listenGroupFoo(String message) {
		logger.info("Received message 'in group foo': " + message);
		latch.countDown();
	}
	
	@KafkaListener(topics = "${message.topic.name}", groupId = "bar", containerFactory = "barKafkaListenerContainerFactory")
    public void listenGroupBar(String message) {
        System.out.println("Received Message 'in group bar': " + message);
        latch.countDown();
    }
	
	/*Retrieve one or more message headers*/
	@KafkaListener(topics = "${message.topic.name}", containerFactory = "headersKafkaListenerContainerFactory")
	public void listenWithHeaders(@Payload String message, 
								  @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		logger.info("Received message 'With Headers': " + message + "from partition: " + partition);
		latch.countDown();
	}
	
	/*Consume messages by adding a custom filter*/
	@KafkaListener(topics = "${filtered.topic.name}", containerFactory = "filterKafkaListenerContainerFactory")
	public void listenWithFilter(String message) {
		logger.info("Received message 'With Filter': " + message);
		filterLatch.countDown();
	}
	
	@KafkaListener(topics = "${greeting.topic.name}", containerFactory = "greetingKafkaListenerContainerFactory")
	public void greetingListener(Greeting greeting) {
		logger.info("Received 'Greeting message': " + greeting.toString());
		greetingLatch.countDown();
	}
	
}
