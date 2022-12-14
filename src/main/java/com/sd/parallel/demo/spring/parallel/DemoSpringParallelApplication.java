package com.sd.parallel.demo.spring.parallel;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import lombok.Value;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.retrytopic.DestinationTopic;

import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

@SpringBootApplication
public class DemoSpringParallelApplication {

	private static final Logger log = LoggerFactory.getLogger(DemoSpringParallelApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(DemoSpringParallelApplication.class, args);
	}

	@KafkaListener(id = "carlos", topics = "parallel-consumer-input-topic", autoStartup = "true")
	void listen(String in) {
		log.info(in);
	}

	/*@Bean
	//public NewTopic topic() {
		return TopicBuilder.name("carlos").partitions(1).replicas(1).build();
	}*/

	@Bean
	ApplicationRunner runner(KafkaListenerEndpointRegistry registry, DefaultKafkaProducerFactory<String, String> pf , ConsumerFactory<String, String> cf,
							 KafkaTemplate<String, String> template) {

		return args -> {
			MessageListener messageListener = (MessageListener) registry.getListenerContainer("carlos")
					.getContainerProperties().getMessageListener();

			final Properties propsConsumer = new Properties();
			propsConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
					"localhost:9092");
			propsConsumer.put(ConsumerConfig.GROUP_ID_CONFIG,
					"KafkaExampleConsumer");
			propsConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class.getName());
			propsConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class.getName());
			propsConsumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
					false);
			Consumer<String, String> consumer = cf.createConsumer("group-c", "","",propsConsumer);

			Producer<String, String> producer = pf.createProducer();

			var options = ParallelConsumerOptions.<String, String>builder()
					.ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
					.consumer(consumer)
					.producer(producer)
					.commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC)
					.maxConcurrency(10)
					.build();
			ParallelStreamProcessor<String, String> processor = ParallelStreamProcessor
					.createEosStreamProcessor(options);
			processor.subscribe(List.of("parallel-consumer-input-topic"));
			processor.pollAndProduce(
					//context -> messageListener.onMessage(context.getSingleConsumerRecord(), null, consumer)
					context -> {
								var consumerRecord = context.getSingleRecord().getConsumerRecord();
								var result = processBrokerRecord(consumerRecord);
								return new ProducerRecord<>("parallel-consumer-output-topic", consumerRecord.key(), result.payload);
							}, consumeProduceResult -> {
								log.debug("Message {} saved to broker at offset {}",
										consumeProduceResult.getOut(),
										consumeProduceResult.getMeta().offset());
							}
					);
			//);
			//IntStream.range(0, 10).forEach(i -> template.send("carlos", "foo" + i));
		};
	}
	private Result processBrokerRecord(ConsumerRecord<String, String> consumerRecord) {
		return new Result("Some payload from " + consumerRecord.value());
	}
	@Value
	static class Result {
		String payload;
	}
}