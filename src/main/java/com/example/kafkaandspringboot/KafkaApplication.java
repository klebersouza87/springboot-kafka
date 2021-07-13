package com.example.kafkaandspringboot;

import java.util.concurrent.TimeUnit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import com.example.kafkaandspringboot.dto.Greeting;
import com.example.kafkaandspringboot.message.consumer.MessageConsumer;
import com.example.kafkaandspringboot.message.producer.MessageProducer;

@SpringBootApplication
public class KafkaApplication {
	
	@Bean
    public MessageProducer messageProducer() {
        return new MessageProducer();
    }
	
	@Bean
	public MessageConsumer messageListener() {
		return new MessageConsumer();
	}
	
	public static void main(String[] args) throws InterruptedException {
		ConfigurableApplicationContext context = SpringApplication.run(KafkaApplication.class, args);
		
		MessageProducer producer = context.getBean(MessageProducer.class);
		MessageConsumer consumer = context.getBean(MessageConsumer.class);
		
		/*
         * Sending a Hello World message to topic 'firstTopic'. 
         * Must be received by both listeners with group 'foo'
         * and 'bar' with containerFactory fooKafkaListenerContainerFactory
         * and barKafkaListenerContainerFactory respectively.
         * It will also be received by the listener with
         * headersKafkaListenerContainerFactory as container factory.
         */
        producer.sendMessage("Hello, World!");
        consumer.latch.await(3, TimeUnit.SECONDS);

        /*
         * Sending message to a topic with 5 partitions,
         * each message to a different partition. But as per
         * listener configuration, only the messages from
         * partition 0 and 3 will be consumed.
         */
        for (int i = 0; i < 5; i++) {
            producer.sendMessageToPartition("Hello To Partitioned Topic!", i);
        }
        consumer.partitionLatch.await(3, TimeUnit.SECONDS);

        /*
         * Sending message to 'filtered' topic. As per listener
         * configuration,  all messages with char sequence
         * 'World' will be discarded.
         */
        producer.sendMessageToFiltered("Hello My Friend!");
        producer.sendMessageToFiltered("Hello new World!");
        consumer.filterLatch.await(3, TimeUnit.SECONDS);

        /*
         * Sending message to 'greeting' topic. This will send
         * and received a java object with the help of
         * greetingKafkaListenerContainerFactory.
         */
        producer.sendGreetingMessage(new Greeting("Greetings", "World!"));
        consumer.greetingLatch.await(3, TimeUnit.SECONDS);

        context.close();
	}

}
