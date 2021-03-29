package com.fizzed.transactional;

public interface ServiceTransactionAdapter {

    void rollback();

    void commit();
    
}
