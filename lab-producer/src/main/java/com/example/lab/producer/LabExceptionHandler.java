package com.example.lab.producer;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.http.converter.HttpMessageNotReadableException;

/**
 * Renders the spec's 422 shape: {"errors":[{"field","reason"}]}. Validation
 * rejections from LabController and malformed JSON bodies both map to 422,
 * NOT to a 500 stack trace.
 */
@RestControllerAdvice
class LabExceptionHandler {

    @ExceptionHandler(LabController.UnprocessableEntityException.class)
    ResponseEntity<LabController.UnprocessableEntityBody> handleValidation(
            LabController.UnprocessableEntityException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(new LabController.UnprocessableEntityBody(ex.getErrors()));
    }

    /** Malformed JSON body -> 422, not a 500 stack trace (spec scenario). */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<LabController.UnprocessableEntityBody> handleMalformedJson(
            HttpMessageNotReadableException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(new LabController.UnprocessableEntityBody(
                        List.of(new ValidationFailure("body", "request body is not valid JSON"))));
    }
}
