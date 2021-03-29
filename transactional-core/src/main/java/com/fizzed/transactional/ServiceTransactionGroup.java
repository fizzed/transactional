package com.fizzed.transactional;

import com.fizzed.crux.util.StopWatch;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceTransactionGroup {
    static private final Logger log = LoggerFactory.getLogger(ServiceTransactionGroup.class);
    
    private final long id;
    private final StopWatch timer;
    private final List<ServiceTransaction> transactions;
    
    public ServiceTransactionGroup(long id) {
        this.id = id;
        this.timer = StopWatch.timeMillis();
        this.transactions = new ArrayList<>();
    }

    public long getId() {
        return id;
    }

    public StopWatch getTimer() {
        return timer;
    }
    
    public boolean hasTransactions() {
        return this.transactions != null && !this.transactions.isEmpty();
    }
    
    public ServiceTransaction begin(
            String descriptor,
            String idempotency,
            Function<Boolean,ServiceTransactionAdapter> supplier) {
        
        final int index = this.transactions.size();
        
        // is this the first in the series of idempotency?
        boolean first = idempotency == null
            || this.transactions.isEmpty()
            || this.transactions.stream().noneMatch(v -> idempotency.equals(v.getIdempotency()));
        
        final ServiceTransactionAdapter adapter = supplier.apply(first);
        
        ServiceTransaction transaction = new ServiceTransaction(
            this, index, idempotency, descriptor, adapter, first);
        
        this.transactions.add(transaction);
        
        log.debug("Transaction begin: group={}, index={}, idempotency={}, first={} ({})",
            id, index, idempotency, first, descriptor);
        
        return transaction;
    }
    
    void commit(int index) {
        // first transaction only triggers final commit
        if (index > 0) {
            return;
        }
        
        log.debug("Transaction commit: group={}", this.id);
        
        try {
            // verify all transactions are ready to commit
            boolean readyForRealCommit = true;
            for (int i = 0; i < this.transactions.size(); i++) {
                ServiceTransaction tr = this.transactions.get(i);
                if (!tr.isReadyForRealCommit()) {
                    readyForRealCommit = false;
                    break;
                }
            }

            if (!readyForRealCommit) {
                this.rollback(index);
                return;
            }

    //        int commits = 0;
    //        int rollbacks = 0;
            boolean rollback = false;

            for (int i = this.transactions.size() - 1; i >= 0; i--) {
                ServiceTransaction tr = this.transactions.get(i);
                if (rollback) {
                    log.debug("Transaction real rollback: group={}, index={} ({})",
                            this.id, tr.getIndex(), tr.getDescriptor());
                    tr.realRollback();
    //                rollbacks++;
                }
                else {
                    try {
                        log.debug("Transaction real commit: group={}, index={} ({})",
                            this.id, tr.getIndex(), tr.getDescriptor());
                        tr.realCommit();
    //                    commits++;
                    } catch (Exception e) {
                        log.warn("Unable to commit (will rollback rest of transaction group)", e);
                        rollback = true;
    //                    rollbacks++;
                    }
                }
            }
        }
        finally {
            // the current transaction MUST be completed
            ServiceTransactions.clear();
        }
    }
    
    void rollback(int index) {
        // first transaction only triggers final rollback
        if (index > 0) {
            return;
        }
        
        log.debug("Transaction rollback: group={}", this.id);
        
        try {
            // rollback in reverse order
            for (int i = this.transactions.size() - 1; i >= 0; i--) {
                
                ServiceTransaction tr = this.transactions.get(i);
                
                log.debug("Transaction real rollback: group={}, index={} ({})",
                    this.id, tr.getIndex(), tr.getDescriptor());
                
                tr.realRollback();
            }
        }
        finally {
            ServiceTransactions.clear();
        }
    }
    
}