package com.fizzed.transactional;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ServiceTransactions {
 
    static private final AtomicLong IDENTIFIERS = new AtomicLong();
    static private final ThreadLocal<ServiceTransactionGroup> TL = new ThreadLocal<ServiceTransactionGroup>() {
        @Override
        protected ServiceTransactionGroup initialValue()
        {
            return new ServiceTransactionGroup(IDENTIFIERS.getAndIncrement());
        }
    };
    
    static public ServiceTransaction begin(
            String descriptor) {
        
        return begin(descriptor, null, realCommit -> new ServiceTransactionNoopAdapter());
    }
    
    static public ServiceTransaction begin(
            String descriptor,
            Function<Boolean, ServiceTransactionAdapter> supplier) {
        
        return begin(descriptor, null, supplier);
    }
    
    static public ServiceTransaction begin(
            String descriptor,
            String idempotency,
            Function<Boolean, ServiceTransactionAdapter> supplier) {
        
        return TL.get().begin(descriptor, idempotency, supplier);
    }
    
    // package-level for testing...
    static boolean isActive() {
        return TL.get().hasTransactions();
    }
    
    static public void clear() {
        TL.remove();
    }
    
}