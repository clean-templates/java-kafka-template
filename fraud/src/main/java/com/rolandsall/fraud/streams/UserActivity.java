package com.rolandsall.fraud.streams;

import com.rolandsall.fraud.model.Transaction;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static com.rolandsall.fraud.streams.FraudDetectionStreamProcessor.AMOUNT_SUM_THRESHOLD;
import static com.rolandsall.fraud.streams.FraudDetectionStreamProcessor.TRANSACTION_COUNT_THRESHOLD;

public class UserActivity {
    private List<Transaction> transactions = new ArrayList<>();
    private Transaction lastTransaction;
    private long lastUpdated = System.currentTimeMillis();

    // Default constructor for Jackson deserialization
    public UserActivity() {}

    public void addTransaction(Transaction transaction) {
        transactions.add(transaction);
        lastTransaction = transaction;
        lastUpdated = System.currentTimeMillis();
    }

    public boolean isHighRiskActivity() {
        return getCount() > TRANSACTION_COUNT_THRESHOLD || getTotalAmount() > AMOUNT_SUM_THRESHOLD;
    }

    private List<Transaction> getRecentTransactions() {
        LocalDateTime tenMinutesAgo = LocalDateTime.now().minus(10, ChronoUnit.MINUTES);
        return transactions.stream()
                .filter(t -> t.getTimestamp().isAfter(tenMinutesAgo))
                .toList();
    }

    // Getters for fields
    public int getCount() {
        return getRecentTransactions().size();
    }

    public double getTotalAmount() {
        return getRecentTransactions().stream()
                .mapToDouble(Transaction::getAmount)
                .sum();
    }

    public Transaction getLastTransaction() {
        return lastTransaction;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }
}
