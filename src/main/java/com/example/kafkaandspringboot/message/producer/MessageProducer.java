package com.example.kafkaandspringboot.message.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.example.kafkaandspringboot.dto.Greeting;

public class MessageProducer {
	
	private Logger logger = LoggerFactory.getLogger(MessageProducer.class);
	
	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;
	
	@Autowired
	KafkaTemplate<String, Greeting> greetingKafkaTemplate;
	
	@Value(value = "${message.topic.name}")
	private String topicName;
	
	@Value(value = "${greeting.topic.name}")
    private String greetingTopicName;
	
	@Value(value = "${partitioned.topic.name}")
	private String partitionedTopicName;
	
	@Value(value = "${filtered.topic.name}")
    private String filteredTopicName;
		
	public void sendMessage(String message) {
		ListenableFuture<SendResult<String,String>> future = kafkaTemplate.send(topicName, message);
		
		/*handle the results asynchronously so that the subsequent messages do not wait for the result of the previous message*/
		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

			@Override
			public void onSuccess(SendResult<String, String> result) {
				logger.info("Sent message=[ " + message + " ] with offset=[ " 
						+ result.getRecordMetadata().offset() + " ]");
			}

			@Override
			public void onFailure(Throwable ex) {
				logger.info("Unable to send message=[ " + message + " ] due to: " + ex.getMessage());
			}
		});
	}
	
	public void sendGreetingMessage(Greeting greeting) {
		greetingKafkaTemplate.send(greetingTopicName, greeting);
	}
	
	public void sendMessageToPartition(String message, int partition) {
		kafkaTemplate.send(partitionedTopicName, partition, null, message);
	}
	
	public void sendMessageToFiltered(String message) {
        kafkaTemplate.send(filteredTopicName, message);
    }
}
