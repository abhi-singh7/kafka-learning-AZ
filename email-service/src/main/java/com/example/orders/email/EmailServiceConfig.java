package com.example.orders.email;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class EmailServiceConfig {

    @Bean
    public MockMailSink mockMailSink() {
        return new MockMailSink(MockMailSink.DEFAULT_CAPACITY);
    }
}
