package pl.poznachowski.kafkaheaderssample;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@SpringBootApplication
public class KafkaHeadersSampleApplication implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(KafkaHeadersSampleApplication.class);
    private static final String TOPIC_NAME = "aTopic";
    private final KafkaTemplate<String, SomePayload> kafkaTemplate;

    public KafkaHeadersSampleApplication(KafkaTemplate<String, SomePayload> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaHeadersSampleApplication.class, args);
    }

    @Bean
    NewTopics topics() {
        return new NewTopics(new NewTopic(TOPIC_NAME, 1, (short) 1));
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Sending record");
        var producerRecord = new ProducerRecord<>(TOPIC_NAME, null, "aKey", new SomePayload("payload"),
                List.of(
                        new RecordHeader("testHeader", "value1".getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader("testHeader", "value2".getBytes(StandardCharsets.UTF_8))
                ));
        kafkaTemplate.send(producerRecord);
    }

    private record SomePayload(String value) {
    }

//    @Component
//    static class ConsumerRecordListener {
//        private static final Logger log = LoggerFactory.getLogger(PayloadListener.class);
//
//        @KafkaListener(topics = TOPIC_NAME)
//        public void listen(ConsumerRecord<String, SomePayload> consumerRecord) {
//
//            log.info("ConsumerRecord payload: {}, headers: {}", consumerRecord.value(), consumerRecord.headers());
//        }
//    }

    @Component
    static class PayloadListener {
        private static final Logger log = LoggerFactory.getLogger(PayloadListener.class);

        @KafkaListener(topics = TOPIC_NAME)
        public void listen(@Payload SomePayload payload, @Headers Map<String, Object> headers) {

            log.info("payload: {}", payload);
            log.info("headers: {}", headers);
        }
    }
}
