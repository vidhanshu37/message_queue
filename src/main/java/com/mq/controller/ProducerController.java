package com.mq.controller;

import com.mq.dto.request.ProduceRequest;
import com.mq.dto.response.ProduceResponse;
import com.mq.service.ProducerService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/v1/producer")
@RequiredArgsConstructor
public class ProducerController {
    private final ProducerService producerService;

    // return 201 - created with location of the produced message (topic, partition, offset)
    @PostMapping("/produce")
    public ResponseEntity<ProduceResponse> produce(@Valid @RequestBody ProduceRequest request) {
        ProduceResponse response = producerService.produce(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}
