package com.example.lab.producer;

import java.util.List;

/**
 * POST /lab/orders request body (same contract as the sibling order-service):
 * {"customerId":"acme","items":[{"sku":"a","qty":1,"price":1.0}],"orderId":"optional"}.
 * A client-supplied orderId is accepted on the wire but ignored; the service
 * generates its own UUID.
 */
public record OrderRequestDto(
        String customerId,
        List<LineDto> items,
        @jakarta.annotation.Nullable String orderId // read, then discarded
) {
}
