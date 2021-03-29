package com.fizzed.transactional;

public class ServiceTransactionNoopAdapter implements ServiceTransactionAdapter {

    @Override
    public void rollback() {
        // do nothing
    }

    @Override
    public void commit() {
        // do nothing
    }
    
}
