package com.bank.dispatch_consumer.infrastructure.kafka;

import com.bank.dispatch_consumer.config.KafkaTopicsProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

@Slf4j
@Component
public class DltProducer {

    private final KafkaSender<String, String> sender;
    private final KafkaTopicsProperties topics;

    public DltProducer(SenderOptions<String, String> senderOptions, KafkaTopicsProperties topics) {
        this.sender = KafkaSender.create(senderOptions);
        this.topics = topics;
    }

    public Mono<Void> send(String key, String value) {
        return sender.send(Mono.just(SenderRecord.create(new ProducerRecord<>(topics.getDlt(), key, value), null)))
                .then();
    }
}
