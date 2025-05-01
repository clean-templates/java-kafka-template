package com.rolandsall.fraud.model;

import java.time.LocalDateTime;

public class Transaction {
    private String transactionId;
    private String userId;
    private double amount;
    private String location;
    private LocalDateTime timestamp;
    private String deviceId;
    private boolean isFraudulent;

    // Default constructor for Jackson
    public Transaction() {}

    public Transaction(String transactionId, String userId, double amount, String location, LocalDateTime timestamp, String deviceId) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.location = location;
        this.timestamp = timestamp;
        this.deviceId = deviceId;
    }

    public Transaction(String userId, double amount, String location, LocalDateTime timestamp, String deviceId) {
        this.transactionId = java.util.UUID.randomUUID().toString();
        this.userId = userId;
        this.amount = amount;
        this.location = location;
        this.timestamp = timestamp;
        this.deviceId = deviceId;
    }

    public Transaction(String transactionId, String userId, double amount, String location, String deviceId, String merchantId, LocalDateTime timestamp, boolean isFraudulent) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.location = location;
        this.deviceId = deviceId;
        this.timestamp = timestamp;
        this.isFraudulent = isFraudulent;
    }

    public boolean isFraudulent() {
        return isFraudulent;
    }

    public void setFraudulent(boolean fraudulent) {
        isFraudulent = fraudulent;
    }

    // Getters and setters
    public String getUserId() {
        return userId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "userId='" + userId + '\'' +
                ", amount=" + amount +
                ", location='" + location + '\'' +
                ", timestamp=" + timestamp +
                ", deviceId='" + deviceId + '\'' +
                '}';
    }
}
