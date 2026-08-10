package com.bank.dispatch_consumer.domain.mapper;

import com.bank.dispatch_consumer.domain.entity.CardReplacementEntity;
import com.bank.dispatch_consumer.domain.model.CardReplacementEvent;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class EntityMapper {

    public CardReplacementEvent toEvent(GenericRecord gr) {
        if (gr == null) {
            return null;
        }
        CardReplacementEvent e = new CardReplacementEvent();
        e.setEventId(asString(gr.get("eventId")));
        e.setRequestId(asString(gr.get("requestId")));
        e.setCustomerId(asString(gr.get("customerId")));
        e.setCardPANMasked(asString(gr.get("cardPANMasked")));
        e.setReasonCode(asString(gr.get("reasonCode")));
        e.setPriority(asString(gr.get("priority")));
        e.setBranchCode(asString(gr.get("branchCode")));
        e.setDeliveryAddress(asString(gr.get("deliveryAddress")));
        Object ts = gr.get("requestedAt");
        if (ts instanceof Long l) {
            e.setRequestedAt(Instant.ofEpochMilli(l));
        }
        Object at = gr.get("attemptNumber");
        if (at instanceof Integer i) {
            e.setAttemptNumber(i);
        }
        e.setCorrelationId(asString(gr.get("correlationId")));
        e.setStatus(asString(gr.get("status")));
        return e;
    }

    private String asString(Object value) {
        return value == null ? null : value.toString();
    }

    public CardReplacementEntity toEntity(CardReplacementEvent ev) {
        if (ev == null) {
            return null;
        }
        return CardReplacementEntity.builder()
                .requestId(ev.getRequestId())
                .customerId(ev.getCustomerId())
                .cardPANMasked(ev.getCardPANMasked())
                .reasonCode(ev.getReasonCode())
                .priority(ev.getPriority())
                .branchCode(ev.getBranchCode())
                .deliveryAddress(ev.getDeliveryAddress())
                .requestedAt(ev.getRequestedAt() != null ? ev.getRequestedAt().toEpochMilli() : 0L)
                .attemptNumber(ev.getAttemptNumber() == null ? 1 : ev.getAttemptNumber())
                .correlationId(ev.getCorrelationId())
                .status(ev.getStatus())
                .build();
    }
}
