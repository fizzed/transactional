package com.fizzed.transactional;

import javax.persistence.OptimisticLockException;
import javax.persistence.PersistenceException;
import org.slf4j.Logger;

public class OptimisticTransactions {

//    static public interface Method<T> {
//        T apply() throws RepositoryException;
//    }
//    
//    static public <T> T retryableExecute(Logger logger, int attempts, String message, Method<T> method) throws RepositoryException {
//        PersistenceException exception = null;
//        for (int attempt = 0; attempt < attempts; attempt++) {
//            try {
//                T value = method.apply();
//                if (attempt > 0) {
//                    logger.warn("Attempt #{} succeeded to {}", attempt, message);
//                }
//                return value;
//            } catch (PersistenceException e) {
//                exception = e;
//                if (e instanceof OptimisticLockException) {
//                    logger.warn("Optimistic lock exception on attempt #{} to {}", attempt, message);
//                } else if (e.getMessage() != null & e.getMessage().contains("Duplicate entry")) {
//                    logger.warn("Optimistic create exception on attempt #{} to {}", attempt, message);
//                } else {
//                    break;
//                }
//            }
//        }
//        throw new EntityTransactionException(exception);
//    }
    
    
    static public interface Method {
        void apply() throws Exception;
    }
    
    static public void retryable(Logger logger, String message, int attempts, Method method) throws Exception {
        PersistenceException exception = null;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                method.apply();
                if (attempt > 1) {
                    logger.info("{} recovered from optimistic exception :-) (on attempt {}/{})", message, attempt, attempts);
                }
                return;
            } catch (PersistenceException e) {
                exception = e;
                if (e instanceof OptimisticLockException) {
                    logger.warn("{} optimistic lock exception (on attempt {}/{}) ({})", message, attempt, attempts, e.getMessage());
                } else if (e.getMessage() != null & e.getMessage().contains("Duplicate entry")) {
                    logger.warn("{} optimistic duplicate exception (on attempt {}/{}) ({})", message, attempt, attempts, e.getMessage());
                } else {
                    throw e;
                }
            }
        }
        if (exception != null) {
            logger.warn("{} unable to recover from optimistic lock exception after {} attempts", message, attempts);
            throw exception;
        }
    }
    
}
