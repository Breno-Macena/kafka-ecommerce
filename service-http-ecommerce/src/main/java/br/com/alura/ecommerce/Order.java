package br.com.alura.ecommerce;

import java.math.BigDecimal;

@SuppressWarnings("FieldCanBeLocal")
public class Order {
    private final String orderId, email;
    private final BigDecimal amount;

    public Order(String orderId, String email, BigDecimal amount) {
        this.orderId = orderId;
        this.email = email;
        this.amount = amount;
    }
}
