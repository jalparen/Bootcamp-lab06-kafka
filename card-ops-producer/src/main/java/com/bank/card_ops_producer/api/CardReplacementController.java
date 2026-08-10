package com.bank.card_ops_producer.api;

import com.bank.card_ops_producer.api.dto.CardReplacementRequestDto;
import com.bank.card_ops_producer.domain.service.EventService;
import io.reactivex.rxjava3.core.Single;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/card-replacements")
public class CardReplacementController {

    private final EventService service;

    public CardReplacementController(EventService service) {
        this.service = service;
    }

    @PostMapping
    public Single<ResponseEntity<String>> create(@Valid @RequestBody CardReplacementRequestDto dto) {
        return service.process(dto)
                .map(id -> ResponseEntity.accepted().body(id));
    }
}
