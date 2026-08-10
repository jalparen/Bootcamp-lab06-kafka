package com.bank.dispatch_consumer.infrastructure.kafka;

import com.bank.dispatch_consumer.config.KafkaTopicsProperties;
import com.bank.dispatch_consumer.domain.mapper.EntityMapper;
import com.bank.dispatch_consumer.domain.model.CardReplacementEvent;
import com.bank.dispatch_consumer.domain.service.ProcessingService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaRxConsumer {

    private final ReceiverOptions<String, Object> receiverOptions;
    private final KafkaTopicsProperties topics;
    private final EntityMapper mapper;
    private final ProcessingService processingService;

    private Disposable subscription;

    @PostConstruct
    public void start() {
        subscription = KafkaReceiver.create(
                        receiverOptions.subscription(Collections.singleton(topics.getMain())))
                .receive()
                .map(this::mapRecord)
                .concatMap(msg -> Flux.from(processingService.process(msg).toFuture()))
                .doOnError(error -> log.error("Error en la recepcion de Kafka: {}", error.getMessage()))
                .subscribe(ignored -> { }, error -> log.error("Consumer detenido por error", error));
    }

    private EventMessage<CardReplacementEvent> mapRecord(ReceiverRecord<String, Object> rec) {
        Object value = rec.value();
        CardReplacementEvent event = null;
        try {
            if (value instanceof GenericRecord gr) {
                event = mapper.toEvent(gr);
            } else {
                log.warn("Valor no Avro ({}), se ignora", value == null ? "null" : value.getClass());
            }
        } catch (Exception e) {
            log.error("Error mapeando Avro -> Event", e);
        }
        return new EventMessage<>(event, rec.receiverOffset(), value);
    }

    @PreDestroy
    public void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }
}
