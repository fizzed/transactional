package com.fizzed.transactional;

public interface ServiceTransactionListener {
 
    void onComplete(boolean success);
    
}