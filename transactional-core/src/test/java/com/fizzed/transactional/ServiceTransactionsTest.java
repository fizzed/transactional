package com.fizzed.transactional;

import com.fizzed.transactional.ServiceTransaction.State;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ServiceTransactionsTest {
 
    @Test
    public void oneLevelCommit() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        str1.commit();
        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(true));
        verify(adapter1, times(1)).commit();
        verify(adapter1, times(0)).rollback();
        assertThat(ServiceTransactions.isActive(), is(false));
    }
 
    @Test
    public void oneLevelRollback() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        str1.rollback();
        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(false));
        verify(adapter1, times(0)).commit();
        verify(adapter1, times(1)).rollback();
        assertThat(ServiceTransactions.isActive(), is(false));
    }
 
    @Test
    public void oneLevelImplicitRollbackIfNotCommitted() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        assertThat(str1.getGroup().isCompleted(), is(false));
        assertThat(ServiceTransactions.isActive(), is(true));
        
        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(false));
        verify(adapter1, times(0)).commit();
        verify(adapter1, times(1)).rollback();
        assertThat(ServiceTransactions.isActive(), is(false));
    }
    
    @Test
    public void oneLevelCommitTriggersRollback() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        final RuntimeException e1 = new RuntimeException("Commit causes rollback");
        doThrow(e1).when(adapter1).commit();
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        assertThat(ServiceTransactions.isActive(), is(true));

        try {
            str1.commit();
            fail();
        }
        catch (Exception e) {
            // expected
        }
        
        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(false));
        assertThat(str1.getCause(), is(e1));
        verify(adapter1, times(1)).commit();
        verify(adapter1, times(0)).rollback();
        assertThat(ServiceTransactions.isActive(), is(false));
    }
    
    @Test
    public void oneLevelCommitTriggersListener() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        final ServiceTransactionListener listener1 = spy(new ServiceTransactionListener() {
            @Override
            public void onComplete(boolean success) {
                System.out.println("onComplete called!");
            }
        });
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        str1.addListener(listener1);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        str1.commit();
        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(true));
        verify(adapter1, times(1)).commit();
        verify(adapter1, times(0)).rollback();
        verify(listener1, times(1)).onComplete(anyBoolean());
        assertThat(ServiceTransactions.isActive(), is(false));
    }
    
    @Test
    public void oneLevelCommitTriggersListenerOnEnd() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        final ServiceTransactionListener listener1 = spy(new ServiceTransactionListener() {
            @Override
            public void onComplete(boolean success) {
                System.out.println("onComplete called!");
            }
        });
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        str1.commit();
        
        str1.addListener(listener1);
        
        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(true));
        verify(adapter1, times(1)).commit();
        verify(adapter1, times(0)).rollback();
        verify(listener1, times(1)).onComplete(anyBoolean());
        assertThat(ServiceTransactions.isActive(), is(false));
    }
    
    @Test
    public void oneLevelCommitTriggersOnSuccess() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        final Runnable listener1 = spy(new Runnable() {
            @Override
            public void run() {
                System.out.println("onComplete called!");
            }
        });
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        str1.onSuccess(listener1);
        
        str1.commit();
        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(true));
        verify(adapter1, times(1)).commit();
        verify(adapter1, times(0)).rollback();
        verify(listener1, times(1)).run();
        assertThat(ServiceTransactions.isActive(), is(false));
    }
    
    @Test
    public void oneLevelRollbackNotTriggersOnSuccess() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        final Runnable listener1 = spy(new Runnable() {
            @Override
            public void run() {
                System.out.println("onComplete called!");
            }
        });
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        str1.onSuccess(listener1);

        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(false));
        verify(adapter1, times(0)).commit();
        verify(adapter1, times(1)).rollback();
        verify(listener1, times(0)).run();
        assertThat(ServiceTransactions.isActive(), is(false));
    }
    
    @Test
    public void twoLevelCommit() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        
        final ServiceTransactionAdapter adapter2 = spy(new ServiceTransactionNoopAdapter());
        
        final ServiceTransaction str2 = ServiceTransactions.begin("test2", (b) -> adapter2);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        
        str2.commit();
        str2.end();
        
        // the commit did not really happen yet
        verify(adapter2, times(0)).commit();
        
        str1.commit();
        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(true));
        verify(adapter2, times(1)).commit();
        verify(adapter2, times(0)).rollback();
        verify(adapter1, times(1)).commit();
        verify(adapter1, times(0)).rollback();
        assertThat(ServiceTransactions.isActive(), is(false));
    }
    
    @Test
    public void twoLevelCommitTriggersListener() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        
        final ServiceTransactionAdapter adapter2 = spy(new ServiceTransactionNoopAdapter());
        
        final ServiceTransaction str2 = ServiceTransactions.begin("test2", (b) -> adapter2);
        
        final ServiceTransactionListener listener2 = spy(new ServiceTransactionListener() {
            @Override
            public void onComplete(boolean success) {
                System.out.println("onComplete called!");
            }
        });
        
        str2.addListener(listener2);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        
        str2.commit();
        str2.end();
        
        // the commit did not really happen yet
        verify(adapter2, times(0)).commit();
        verify(listener2, times(0)).onComplete(anyBoolean());
        
        str1.commit();
        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(true));
        verify(adapter2, times(1)).commit();
        verify(adapter2, times(0)).rollback();
        verify(listener2, times(1)).onComplete(anyBoolean());
        verify(adapter1, times(1)).commit();
        verify(adapter1, times(0)).rollback();
        assertThat(ServiceTransactions.isActive(), is(false));
    }
    
    @Test
    public void twoLevelCommitTriggersListenerAtEnd() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        final ServiceTransactionListener listener1 = spy(new ServiceTransactionListener() {
            @Override
            public void onComplete(boolean success) {
                System.out.println("onComplete called!");
            }
        });
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        
        final ServiceTransactionAdapter adapter2 = spy(new ServiceTransactionNoopAdapter());
        
        final ServiceTransaction str2 = ServiceTransactions.begin("test2", (b) -> adapter2);
        
        final ServiceTransactionListener listener2 = spy(new ServiceTransactionListener() {
            @Override
            public void onComplete(boolean success) {
                System.out.println("onComplete called!");
            }
        });
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        str2.commit();
        
        str2.addListener(listener2);
        
        // should NOT be called yet in the addListener method
        verify(listener2, times(0)).onComplete(anyBoolean());
        
        
        str2.end();
        
        // the commit did not really happen yet
        verify(adapter2, times(0)).commit();
        verify(listener2, times(0)).onComplete(anyBoolean());
        
        str1.commit();
        
        str1.addListener(listener1);
        
        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(true));
        verify(adapter2, times(1)).commit();
        verify(adapter2, times(0)).rollback();
        verify(listener1, times(1)).onComplete(anyBoolean());
        verify(listener2, times(1)).onComplete(anyBoolean());
        verify(adapter1, times(1)).commit();
        verify(adapter1, times(0)).rollback();
        assertThat(ServiceTransactions.isActive(), is(false));
    }
    
    @Test
    public void twoLevelCommitTriggersRollback() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        
        final ServiceTransactionAdapter adapter2 = spy(new ServiceTransactionNoopAdapter());
        final RuntimeException e1 = new RuntimeException("Commit causes rollback");
        doThrow(e1).when(adapter2).commit();
        
        final ServiceTransaction str2 = ServiceTransactions.begin("test2", (b) -> adapter2);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        
        str2.commit();
        str2.end();
        
        try {
            str1.commit();
            fail();
        }
        catch (Exception e) {
            // expected
        }
        
        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(false));
        assertThat(str1.getCause(), is(nullValue()));
        assertThat(str2.getCause(), is(e1));
        verify(adapter2, times(1)).commit();            // causes rollback in real adapter
        verify(adapter2, times(0)).rollback();
        verify(adapter1, times(0)).commit();
        verify(adapter1, times(1)).rollback();          // should be rolled back
        assertThat(ServiceTransactions.isActive(), is(false));
    }
    
    @Test
    public void twoLevelCommitTriggersRollbackWithRollbackFailing() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        final RuntimeException e1 = new RuntimeException("Rollback exception");
        doThrow(e1).when(adapter1).rollback();
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        
        final ServiceTransactionAdapter adapter2 = spy(new ServiceTransactionNoopAdapter());
        final RuntimeException e2 = new RuntimeException("Commit exception");
        doThrow(e2).when(adapter2).commit();
        
        final ServiceTransaction str2 = ServiceTransactions.begin("test2", (b) -> adapter2);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        
        str2.commit();
        str2.end();

        try {
            str1.commit();
            fail();
        } catch (Exception e) {
            // expected
        }
        
        str1.end();
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(false));
        assertThat(str1.getCause(), is(e1));
        assertThat(str2.getCause(), is(e2));
        verify(adapter2, times(1)).commit();            // causes rollback in real adapter
        verify(adapter2, times(0)).rollback();
        verify(adapter1, times(0)).commit();
        verify(adapter1, times(1)).rollback();          // should be rolled back
        assertThat(ServiceTransactions.isActive(), is(false));
    }
    
    @Test
    public void twoLevelRollbackTriggersRollbackWithRollbackFailing() {
        
        final ServiceTransactionAdapter adapter1 = spy(new ServiceTransactionNoopAdapter());
        final RuntimeException e1 = new RuntimeException("Rollback causes exception too");
        doThrow(e1).when(adapter1).rollback();
        
        final ServiceTransaction str1 = ServiceTransactions.begin("test1", (b) -> adapter1);
        
        assertThat(ServiceTransactions.isActive(), is(true));
        
        
        final ServiceTransactionAdapter adapter2 = spy(new ServiceTransactionNoopAdapter());
        final RuntimeException e2 = new RuntimeException("Rollback causes rollback");
        doThrow(e2).when(adapter2).rollback();
        
        final ServiceTransaction str2 = ServiceTransactions.begin("test2", (b) -> adapter2);
        
        assertThat(ServiceTransactions.isActive(), is(true));

        try {
            str2.rollback();
            fail();
        } catch (Exception e) {
            // expected
        }
        
        str2.end();
        
        assertThat(str2.getState(), is(State.ROLLBACK));
        assertThat(str1.getState(), is(State.ACTIVE));

        try {
            str1.rollback();
            fail();
        } catch (Exception e) {
            // expected
        }
        
        str1.end();
        
        assertThat(str2.getState(), is(State.ROLLBACK));
        assertThat(str1.getState(), is(State.ROLLBACK));
        
        assertThat(str1.getGroup().isCompleted(), is(true));
        assertThat(str1.getGroup().wasSuccessful(), is(false));
        verify(adapter2, times(0)).commit();            // causes rollback in real adapter
        verify(adapter2, times(1)).rollback();
        verify(adapter1, times(0)).commit();
        verify(adapter1, times(1)).rollback();          // should be rolled back
        assertThat(ServiceTransactions.isActive(), is(false));
    }
    
}