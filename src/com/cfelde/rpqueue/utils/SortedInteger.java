/*
 * Copyright 2013 Christian Felde (cfelde [at] cfelde [dot] com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.cfelde.rpqueue.utils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Allows us to sort ints while still keeping each instance of an int distinct.
 * If two ints are equal, they are sorted according to when they were put
 * into an instance of SortedInteger.
 *
 * @author cfelde
 */
public class SortedInteger implements Comparable<SortedInteger> {
    private static final AtomicLong SEQ_GENERATOR = new AtomicLong();
    
    private final int value;
    private final long seq = SEQ_GENERATOR.getAndIncrement();
    
    public SortedInteger(int value) {
        this.value = value;
    }
    
    public int getValue() {
        return value;
    }

    @Override
    public int compareTo(SortedInteger other) {
        int cmp = (value < other.value) ? -1 : ((value == other.value) ? 0 : 1);
        
        if (cmp == 0)
            cmp = (seq < other.seq) ? -1 : ((seq == other.seq) ? 0 : 1);
        
        return cmp;
    }
}
