package com.fizzed.transactional;

import javax.persistence.OptimisticLockException;
import javax.persistence.PersistenceException;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fizzed.transactional.OptimisticTransactions.Method;

public class OptimisticTransactionsTest {
    static private final Logger log = LoggerFactory.getLogger(OptimisticTransactionsTest.class);
    
    @Test
    public void retryable() throws Exception {
        Method method;
        
        // only execute 1 time if its not an exception we handle specially
        method = mock(Method.class);
        IllegalArgumentException exception1 = new IllegalArgumentException("test");
        doThrow(exception1).when(method).apply();
        
        try {
            OptimisticTransactions.retryable(log, "test1", 2, method);
            fail();
        } catch (Exception e) {
            assertThat(e, sameInstance(exception1));
            verify(method, times(1)).apply();
        }
        
        // execute exactly X times for special exception
        method = mock(Method.class);
        PersistenceException exception2 = new OptimisticLockException("test");
        doThrow(exception2).when(method).apply();
        
        try {
            OptimisticTransactions.retryable(log, "test2", 3, method);
            fail();
        } catch (Exception e) {
            assertThat(e, sameInstance(exception2));
            verify(method, times(3)).apply();
        }
        
        // execute exactly X times for special exception
        method = mock(Method.class);
        PersistenceException exception3 = new PersistenceException("Duplicate entry");
        doThrow(exception3).when(method).apply();
        
        try {
            OptimisticTransactions.retryable(log, "test3", 3, method);
            fail();
        } catch (Exception e) {
            assertThat(e, sameInstance(exception3));
            verify(method, times(3)).apply();
        }
        
        // execute X times then succeeds
        method = mock(Method.class);
        PersistenceException exception4 = new OptimisticLockException("test");
        doThrow(exception4).doNothing().when(method).apply();
        
        OptimisticTransactions.retryable(log, "test4", 3, method);
        
//        assertThat(val, is("success"));
        verify(method, times(2)).apply();
    }
    
}
