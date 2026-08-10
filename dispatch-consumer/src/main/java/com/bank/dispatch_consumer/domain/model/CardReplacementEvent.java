package com.bank.dispatch_consumer.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CardReplacementEvent {

    private String eventId;
    private String requestId;
    private String customerId;
    private String cardPANMasked;
    private String reasonCode;
    private String priority;
    private String branchCode;
    private String deliveryAddress;
    private Instant requestedAt;
    private Integer attemptNumber;
    private String correlationId;
    private String status;
}
