package br.com.alura.ecommmerce;

import java.math.BigDecimal;

public class Order {
    private final String orderId, email;
    private final BigDecimal amount;

    public Order(String orderId, String email, BigDecimal amount) {
        this.orderId = orderId;
        this.email = email;
        this.amount = amount;
    }
}
