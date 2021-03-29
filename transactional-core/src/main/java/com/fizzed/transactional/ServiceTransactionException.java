package com.fizzed.transactional;

public class ServiceTransactionException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    
    /**
     * Constructs an instance of <code>ServiceTransactionException</code> with
     * the specified detail message.
     *
     * @param msg the detail message.
     */
    public ServiceTransactionException(String msg) {
        super(msg);
    }
    
    public ServiceTransactionException(String msg, Throwable cause) {
        super(msg, cause);
    }
    
}