package com.example.orders.api;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Renders 422 responses in the spec's shape: {"errors":[{"field","reason"}]}.
 */
@RestControllerAdvice
class OrderExceptionHandler {

    @ExceptionHandler(OrderController.UnprocessableEntityException.class)
    ResponseEntity<OrderController.UnprocessableEntityBody> handleValidation(
            OrderController.UnprocessableEntityException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(new OrderController.UnprocessableEntityBody(ex.getErrors()));
    }

    /** Malformed JSON body → 422, not a 500 stack trace (spec scenario). */
    @ExceptionHandler(org.springframework.http.converter.HttpMessageNotReadableException.class)
    ResponseEntity<OrderController.UnprocessableEntityBody> handleMalformedJson(
            org.springframework.http.converter.HttpMessageNotReadableException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(new OrderController.UnprocessableEntityBody(
                        List.of(new OrderValidationError("body", "request body is not valid JSON"))));
    }
}
