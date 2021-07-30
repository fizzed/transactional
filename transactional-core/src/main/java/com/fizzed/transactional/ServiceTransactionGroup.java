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
    private boolean success;
    private List<ServiceTransactionListener> listeners;
    
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
    
    public boolean isCompleted() {
        return this.timer.isStopped();
    }
    
    public boolean wasSuccessful() {
        return this.isCompleted() && this.success;
    }
    
    public boolean hasTransactions() {
        return this.transactions != null && !this.transactions.isEmpty();
    }
    
    public void addListener(ServiceTransactionListener listener) {
        // if the group is already completed, we can run the listener now
        if (this.isCompleted()) {
            listener.onComplete(this.success);
        }
        else {
            if (this.listeners == null) {
                this.listeners = new ArrayList<>();
            }
            this.listeners.add(listener);
        }
    }
    
    public void removeListener(ServiceTransactionListener listener) {
        if (this.listeners != null) {
            this.listeners.remove(listener);
            if (this.listeners.isEmpty()) {
                this.listeners = null;
            }
        }
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
    
    private void complete(boolean success) {
        
        final boolean isFirstComplete = this.timer.isRunning();
        
        this.success = success;
        
        this.timer.stop();
        
        // the current transaction MUST be completed
        ServiceTransactions.clear();
        
        log.debug("Transaction complete: group={} (in {})", this.id, this.timer);
        
        if (isFirstComplete && this.listeners != null) {
            for (ServiceTransactionListener listener : this.listeners) {
                try {
                    listener.onComplete(success);
                }
                catch (Throwable t) {
                    log.error("Unhandled throwable in transaction listener.onComplete!", t);
                }
            }
        }
        
        // re-throw the most inner-most cause
        for (int i = this.transactions.size() - 1; i >= 0; i--) {
            ServiceTransaction tr = this.transactions.get(i);
            if (tr.getCause() != null) {
                throw new ServiceTransactionException("Unable to cleanly execute transaction group=" + this.getId(), tr.getCause());
            }
        }
    }
    
    void commit(int index) {
        // first transaction only triggers final commit
        if (index > 0) {
            return;
        }
        
        log.debug("Transaction commit: group={}", this.id);
        
        boolean rollback = false;
        
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

            for (int i = this.transactions.size() - 1; i >= 0; i--) {
                ServiceTransaction tr = this.transactions.get(i);
                if (rollback) {
                    try {
                        log.debug("Transaction real rollback: group={}, index={} ({})",
                            this.id, tr.getIndex(), tr.getDescriptor());
                        
                        tr.realRollback();
                    }
                    catch (Exception e) {
                        log.warn("Unable to rollback (will continue rolling back rest of transaction group): {}", e.getMessage());
                    }
                }
                else {
                    try {
                        log.debug("Transaction real commit: group={}, index={} ({})",
                            this.id, tr.getIndex(), tr.getDescriptor());
                        
                        tr.realCommit();
                    } catch (Exception e) {
                        log.warn("Unable to commit (will rollback rest of transaction group): {}", e.getMessage());
                        rollback = true;
                    }
                }
            }
        }
        finally {
            this.complete(!rollback);
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
            this.complete(false);
        }
    }
    
}