package com.bank.card_ops_producer.domain.service;

import com.bank.card_ops_producer.api.dto.CardReplacementRequestDto;
import com.bank.card_ops_producer.domain.mapper.EventMapper;
import com.bank.card_ops_producer.domain.policy.AttemptPolicy;
import com.bank.card_ops_producer.domain.port.AttemptStateRepository;
import com.bank.card_ops_producer.domain.port.EventPublisher;
import com.bank.events.CardReplacementEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.Single;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class EventService {

    private static final Duration SNAPSHOT_TTL = Duration.ofMinutes(15);

    private final AttemptPolicy policy;
    private final EventMapper mapper;
    private final EventPublisher<Object> publisher;
    private final AttemptStateRepository stateRepository;
    private final ObjectMapper objectMapper;
    private final String topic;

    public EventService(AttemptPolicy policy,
                        EventMapper mapper,
                        EventPublisher<Object> publisher,
                        AttemptStateRepository stateRepository,
                        ObjectMapper objectMapper,
                        @Value("${kafka.topic}") String topic) {
        this.policy = policy;
        this.mapper = mapper;
        this.publisher = publisher;
        this.stateRepository = stateRepository;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public Single<String> process(CardReplacementRequestDto dto) {
        return policy.resolveAttempt(dto)
                .map(attempt -> mapper.toEvent(dto, attempt))
                .flatMap(evt -> stateRepository.saveEventSnapshot(evt.getRequestId().toString(), toJson(evt), SNAPSHOT_TTL)
                        .flatMap(saved -> publisher.publish(topic, evt.getRequestId().toString(), evt)
                                .map(sr -> evt.getEventId().toString())));
    }

    private String toJson(CardReplacementEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No se pudo serializar el snapshot del evento", e);
        }
    }
}
