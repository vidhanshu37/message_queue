package com.mq.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.support.DefaultMessageSourceResolvable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    // Domain exceptions
    @ExceptionHandler(TopicAlreadyExistsException.class)
    public ResponseEntity<Map<String, Object>> handleDuplicate(TopicAlreadyExistsException ex) {
        return error(HttpStatus.CONFLICT, "TOPIC_ALREADY_EXISTS", ex.getMessage());
    }

    @ExceptionHandler(TopicNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNotFound(TopicNotFoundException ex) {
        return error(HttpStatus.NOT_FOUND, "TOPIC_NOT_FOUND", ex.getMessage());
    }

    // Consumer group
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalState(IllegalStateException ex) {
        String msg = ex.getMessage() != null ? ex.getMessage() : "Illegal state";

        if (msg.contains("generation")) {
            // Zombie consumer — stale generation after rebalance
            log.warn("Stale generation rejected: {}", msg);
            return error(HttpStatus.CONFLICT, "STALE_GENERATION", msg);
        }

        if (msg.contains("rebalancing") || msg.contains("PREPARING_REBALANCE")) {
            // Group mid-rebalance tell client to retry
            log.info("Request during rebalance: {}", msg);
            return error(HttpStatus.SERVICE_UNAVAILABLE, "REBALANCE_IN_PROGRESS", msg);
        }

        if (msg.contains("not a member") || msg.contains("join first") || msg.contains("not found")) {
            // Consumer trying to consume without joining
            log.warn("Unauthorized consumer access: {}", msg);
            return error(HttpStatus.FORBIDDEN, "NOT_A_MEMBER", msg);
        }

        // Unknown illegal state — treat as server error
        log.error("IllegalStateException: {}", msg, ex);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "ILLEGAL_STATE", msg);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex) {
        String msg = ex.getMessage() != null ? ex.getMessage() : "Invalid argument";

        if (msg.contains("not own") || msg.contains("does not own")) {
            // Partition ownership violation
            log.warn("Partition ownership violation: {}", msg);
            return error(HttpStatus.FORBIDDEN, "PARTITION_NOT_OWNED", msg);
        }

        if (msg.contains("not initialized") || msg.contains("Partition not")) {
            // Partition doesn't exist yet on this broker
            log.warn("Partition not initialized: {}", msg);
            return error(HttpStatus.NOT_FOUND, "PARTITION_NOT_FOUND", msg);
        }

        // Generic bad request
        log.warn("Bad request argument: {}", msg);
        return error(HttpStatus.BAD_REQUEST, "INVALID_ARGUMENT", msg);
    }

    // Infrastructure exceptions
    @ExceptionHandler(IOException.class)
    public ResponseEntity<Map<String, Object>> handleIOException(IOException ex) {
        log.error("Storage I/O failure: {}", ex.getMessage(), ex);
        return error(HttpStatus.SERVICE_UNAVAILABLE, "STORAGE_FAILURE",
                "Storage error — retry on another broker: " + ex.getMessage());
    }

    // Validation

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> fieldErrors = ex.getBindingResult().getFieldErrors()
                .stream()
                .collect(Collectors.toMap(
                        FieldError::getField,
                        DefaultMessageSourceResolvable::getDefaultMessage,
                        (a, b) -> a  // keep first on duplicate
                ));

        return ResponseEntity.badRequest().body(Map.of(
                "error", "VALIDATION_FAILED",
                "fields", fieldErrors,
                "timestamp", Instant.now().toString()
        ));
    }

    // Catch-all

    /**
     * Catch-all for anything we didn't anticipate.
     * Log the full stack trace — this should NEVER happen in production.
     * If it does, it means we need a more specific handler above.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleUnexpected(Exception ex) {
        log.error("Unexpected error [{}]: {}", ex.getClass().getSimpleName(), ex.getMessage(), ex);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                "Unexpected error: " + ex.getMessage());
    }

    // Builder
    private ResponseEntity<Map<String, Object>> error(HttpStatus status, String code, String message) {
        return ResponseEntity.status(status).body(Map.of(
                "error", code,
                "message", message != null ? message : "No details available",
                "status", status.value(),
                "timestamp", Instant.now().toString()
        ));
    }
}