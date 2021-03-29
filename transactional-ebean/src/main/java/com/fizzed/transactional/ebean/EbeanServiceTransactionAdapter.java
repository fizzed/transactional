/*
 * Copyright 2021 Fizzed, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fizzed.transactional.ebean;

import com.fizzed.transactional.ServiceTransactionAdapter;
import io.ebean.Transaction;

public class EbeanServiceTransactionAdapter implements ServiceTransactionAdapter {
 
    private final Transaction transaction;

    public EbeanServiceTransactionAdapter(Transaction transaction) {
        this.transaction = transaction;
    }

    public boolean isSafeToIgnore(Exception e) {
        if (e != null) {
            if (e instanceof IllegalStateException) {
                String m = e.getMessage();
                if (m != null && m.toLowerCase().contains("inactive")) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void rollback() {
        try {
            transaction.rollback();
        } catch (IllegalStateException e) {
            if (this.isSafeToIgnore(e)) {
                // do nothing
                return;
            }
            throw e;
        }
    }

    @Override
    public void commit() {
        try {
            transaction.commit();
        } catch (IllegalStateException e) {
            if (this.isSafeToIgnore(e)) {
                // do nothing
                return;
            }
            throw e;
        }
    }
        
}