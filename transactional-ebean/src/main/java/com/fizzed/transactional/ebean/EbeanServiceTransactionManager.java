package com.fizzed.transactional.ebean;

import com.fizzed.transactional.ServiceTransactionAdapter;
import com.fizzed.transactional.ServiceTransactionNoopAdapter;
import io.ebean.EbeanServer;
import io.ebean.Transaction;
import io.ebean.annotation.TxIsolation;
import java.util.function.Function;

public class EbeanServiceTransactionManager {

    private final EbeanServer ebean;

    public EbeanServiceTransactionManager(EbeanServer ebean) {
        this.ebean = ebean;
    }

    public Function<Boolean,ServiceTransactionAdapter> supplier() {
        return this.supplier(TxIsolation.READ_COMMITED);
    }
    
    public Function<Boolean,ServiceTransactionAdapter> supplier(TxIsolation isolation) {
        return (first) -> {
            // only the first transaction can do real begin, rollback, and commit
            if (!first) {
                return new ServiceTransactionNoopAdapter();
            } else {
                final Transaction transaction = this.ebean.beginTransaction(isolation);
                return new EbeanServiceTransactionAdapter(transaction);
            }
        };
    }
    
}