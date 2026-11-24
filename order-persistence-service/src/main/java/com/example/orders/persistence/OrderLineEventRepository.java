package com.example.orders.persistence;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderLineEventRepository extends JpaRepository<OrderLineEvent, Long> {

    List<OrderLineEvent> findByOrderId(String orderId);

    boolean existsByMessageId(String messageId);

    boolean existsByOrderIdAndSku(String orderId, String sku);
}
