package com.jpmc.midascore.consumer;

import com.jpmc.midascore.IncentiveQuerier;
import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    private final DatabaseConduit databaseConduit;
    private final IncentiveQuerier incentiveQuerier;

    public TransactionListener(DatabaseConduit databaseConduit,
                               IncentiveQuerier incentiveQuerier) {
        this.databaseConduit = databaseConduit;
        this.incentiveQuerier = incentiveQuerier;
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group"
    )
    public void receive(Transaction transaction) {

        UserRecord sender = databaseConduit.findUser(transaction.getSenderId());
        UserRecord recipient = databaseConduit.findUser(transaction.getRecipientId());

        // Ignore invalid users
        if (sender == null || recipient == null) {
            return;
        }

        // Ignore if sender doesn't have enough balance
        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        // Call Incentive API
        Incentive incentive = incentiveQuerier.query(transaction);

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());

        recipient.setBalance(
                recipient.getBalance()
                        + transaction.getAmount()
                        + incentive.getAmount()
        );

        // Save updated users
        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        // Save transaction
        databaseConduit.saveTransaction(
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount(),
                        incentive.getAmount()
                )
        );
    }
}