package com.fizzed.transactional;

import static com.fizzed.crux.util.MoreObjects.in;
import java.io.Closeable;
import java.util.function.Consumer;
import java.util.function.Function;

public class ServiceTransaction implements Closeable {
    
    static public enum State {
        ACTIVE,
        ROLLBACK,
        COMMIT_SCHEDULED,
        COMMITTED
    }
    
    private final ServiceTransactionGroup group;
    private final int index;
    private final String idempotency;
    private final String descriptor;
    private final ServiceTransactionAdapter adapter;
    private State state;
    private Throwable cause;
    
    public ServiceTransaction(
            ServiceTransactionGroup group,
            int index,
            String idempotency,
            String descriptor,
            ServiceTransactionAdapter adapter,
            boolean realCommit) {
        
        this.group = group;
        this.index = index;
        this.idempotency = idempotency;
        this.descriptor = descriptor;
        this.adapter = adapter;
        this.state = State.ACTIVE;
    }
    
    String getIdempotency() {
        return idempotency;
    }

    ServiceTransactionAdapter getAdapter() {
        return adapter;
    }

    public int getIndex() {
        return index;
    }

    public State getState() {
        return state;
    }

    public String getDescriptor() {
        return descriptor;
    }

    public ServiceTransactionGroup getGroup() {
        return group;
    }

    public Throwable getCause() {
        return cause;
    }

    @Override
    public void close() {
        this.end();
    }
    
    public void rollback() {
        if (in(this.state, State.ROLLBACK)) {
            return;
        }
        
        if (!in(this.state, State.ACTIVE)) {
            throw new IllegalStateException("Unable to rollback (transaction state is " + this.state + ")");
        }

        try {
            this.realRollback();
        }
        finally {
            this.group.rollback(this.index);
        }
    }
    
    public void commit() {
        if (!in(this.state, State.ACTIVE, State.COMMIT_SCHEDULED)) {
            throw new IllegalStateException("Unable to commit (transaction state is " + this.state + ")");
        }
        
        this.state = State.COMMIT_SCHEDULED;
        
        this.group.commit(this.index);
    }
    
    public void end() {
        if (!in(this.state, State.COMMIT_SCHEDULED, State.COMMITTED)) {
            // this is an implicit rollback :-(
            this.rollback();
        }
    }
    
    boolean isReadyForRealCommit() {
        return in(this.state, State.COMMIT_SCHEDULED, State.COMMITTED);
    }
    
    void realRollback() {
        // defend against multiple calls
        if (in(this.state, State.ROLLBACK)) {
            return;
        }
        
        try {
            this.adapter.rollback();
        } finally {
            this.state = State.ROLLBACK;
        }
    }
    
    void realCommit() throws Exception {
        // defend against multiple calls
        if (in(this.state, State.COMMITTED)) {
            return;
        }
        
        if (!in(this.state, State.ACTIVE, State.COMMIT_SCHEDULED)) {
            throw new IllegalStateException("Unable to truly commit (transaction state is " + this.state + ")");
        }
        
        try {
            this.adapter.commit();
            this.state = State.COMMITTED;
        } catch (Exception e) {
            // if a commit fails, its implied it was already rolled back
            this.state = State.ROLLBACK;
            throw e;
        }
    }
    
    public void execute(Consumer<ServiceTransaction> executor) {
        try {
            executor.accept(this);
        } finally {
            this.end();
        }
    }
    
    public <T> T execute(Function<ServiceTransaction,T> executor) {
        try {
            return executor.apply(this);
        } finally {
            this.end();
        }
    }
    
}