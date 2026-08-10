package com.bank.card_ops_producer.api.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class CardReplacementRequestDto {

    @NotBlank private String requestId;
    @NotBlank private String customerId;
    @NotBlank private String cardPANMasked;
    @NotBlank private String reasonCode;
    @NotBlank private String priority;
    @NotBlank private String branchCode;
    @NotBlank private String deliveryAddress;
    @Builder.Default private Instant requestedAt = Instant.now();
    @NotBlank private String correlationId;
    @NotBlank private String status;
}
