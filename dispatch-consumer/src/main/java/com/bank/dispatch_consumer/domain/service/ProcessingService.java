package com.bank.dispatch_consumer.domain.service;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import com.bank.dispatch_consumer.domain.mapper.EntityMapper;
import com.bank.dispatch_consumer.domain.model.CardReplacementEvent;
import com.bank.dispatch_consumer.domain.repo.CardReplacementRepository;
import com.bank.dispatch_consumer.domain.repo.SnapshotCacheRepository;
import com.bank.dispatch_consumer.infrastructure.kafka.DltProducer;
import com.bank.dispatch_consumer.infrastructure.kafka.EventMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessingService {

    private static final String STATUS_DISPATCHED = "DISPATCHED";
    private static final String STATUS_DISPATCHED_CACHE = "DISPATCHED_CACHE";

    private final EntityMapper mapper;
    private final CardReplacementRepository repo;
    private final SnapshotCacheRepository snapshotCache;
    private final DltProducer dltProducer;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public Completable process(EventMessage<CardReplacementEvent> msg) {
        Timer.Sample sample = Timer.start(meterRegistry);
        CardReplacementEvent event = msg.getPayload();

        if (event == null || event.getRequestId() == null || event.getRequestId().isBlank()) {
            return sendToDlt(msg, "evento invalido o sin requestId", sample);
        }

        return handle(event)
                .doOnSuccess(entity -> {
                    msg.ack();
                    meterRegistry.counter("events_consumed_ok").increment();
                    sample.stop(meterRegistry.timer("process_time"));
                    log.info("Evento procesado requestId={} attempt={} status={}",
                            entity.getRequestId(), entity.getAttemptNumber(), entity.getStatus());
                })
                .ignoreElement()
                .onErrorResumeNext(error -> {
                    log.error("Error procesando requestId={}: {}", event.getRequestId(), error.getMessage());
                    return sendToDlt(msg, error.getMessage(), sample);
                });
    }

    private Single<CardReplacementEntity> handle(CardReplacementEvent event) {
        if (event.getAttemptNumber() != null && event.getAttemptNumber() >= 2) {
            return snapshotCache.getSnapshotJson(event.getRequestId())
                    .defaultIfEmpty(null)
                    .toSingle()
                    .flatMap(snapshotJson -> {
                        CardReplacementEntity entity = mapper.toEntity(event);
                        if (snapshotJson != null && !snapshotJson.isBlank()) {
                            enrichFromSnapshot(entity, snapshotJson);
                            entity.setStatus(STATUS_DISPATCHED_CACHE);
                        } else {
                            entity.setStatus(STATUS_DISPATCHED);
                        }
                        entity.setReceivedAt(System.currentTimeMillis());
                        entity.setProcessedAt(System.currentTimeMillis());
                        entity.setRawPayload(toJson(event));
                        return repo.save(entity);
                    });
        }

        return repo.existsByRequestId(event.getRequestId())
                .flatMap(exists -> {
                    if (exists) {
                        return repo.findByRequestId(event.getRequestId())
                                .defaultIfEmpty(mapper.toEntity(event))
                                .toSingle();
                    }
                    CardReplacementEntity entity = mapper.toEntity(event);
                    entity.setStatus(STATUS_DISPATCHED);
                    entity.setReceivedAt(System.currentTimeMillis());
                    entity.setProcessedAt(System.currentTimeMillis());
                    entity.setRawPayload(toJson(event));
                    return repo.save(entity);
                });
    }

    private void enrichFromSnapshot(CardReplacementEntity entity, String snapshotJson) {
        try {
            JsonNode snap = objectMapper.readTree(snapshotJson);
            entity.setCustomerId(snap.path("customerId").asText(entity.getCustomerId()));
            entity.setCardPANMasked(snap.path("cardPANMasked").asText(entity.getCardPANMasked()));
            entity.setReasonCode(snap.path("reasonCode").asText(entity.getReasonCode()));
            entity.setPriority(snap.path("priority").asText(entity.getPriority()));
            entity.setBranchCode(snap.path("branchCode").asText(entity.getBranchCode()));
            entity.setDeliveryAddress(snap.path("deliveryAddress").asText(entity.getDeliveryAddress()));
            if (snap.hasNonNull("requestedAt")) {
                entity.setRequestedAt(snap.path("requestedAt").asLong(entity.getRequestedAt()));
            }
            if (snap.hasNonNull("correlationId")) {
                entity.setCorrelationId(snap.path("correlationId").asText(entity.getCorrelationId()));
            }
            if (snap.hasNonNull("status")) {
                entity.setStatus(snap.path("status").asText(entity.getStatus()));
            }
        } catch (JsonProcessingException e) {
            log.warn("No se pudo parsear snapshot, se usa solo el evento: {}", e.getMessage());
        }
    }

    private Completable sendToDlt(EventMessage<CardReplacementEvent> msg, String reason, Timer.Sample sample) {
        String key = "unknown";
        if (msg.getPayload() != null && msg.getPayload().getRequestId() != null) {
            key = msg.getPayload().getRequestId();
        }
        String value = msg.getRawKafkaValue() != null ? String.valueOf(msg.getRawKafkaValue()) : reason;
        meterRegistry.counter("events_consumed_err").increment();
        return RxJava3Adapter.monoToCompletable(dltProducer.send(key, value)
                .doOnSuccess(v -> {
                    log.warn("Mensaje reenviado a DLT, key={}, reason={}", key, reason);
                    msg.ack();
                    sample.stop(meterRegistry.timer("process_time"));
                })
                .doOnError(error -> log.error("No se pudo enviar a DLT key={}: {}", key, error.getMessage())));
    }

    private String toJson(CardReplacementEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }
}
