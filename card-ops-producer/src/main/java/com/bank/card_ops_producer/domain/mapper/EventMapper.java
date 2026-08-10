package com.bank.card_ops_producer.domain.mapper;

import com.bank.card_ops_producer.api.dto.CardReplacementRequestDto;
import com.bank.events.CardReplacementEvent;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class EventMapper {

    public CardReplacementEvent toEvent(CardReplacementRequestDto dto, int attemptNumber) {
        return CardReplacementEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setRequestId(dto.getRequestId())
                .setCustomerId(dto.getCustomerId())
                .setCardPANMasked(dto.getCardPANMasked())
                .setReasonCode(dto.getReasonCode())
                .setPriority(dto.getPriority())
                .setBranchCode(dto.getBranchCode())
                .setDeliveryAddress(dto.getDeliveryAddress())
                .setRequestedAt(dto.getRequestedAt())
                .setAttemptNumber(attemptNumber)
                .setCorrelationId(dto.getCorrelationId())
                .setStatus(dto.getStatus())
                .build();
    }
}
