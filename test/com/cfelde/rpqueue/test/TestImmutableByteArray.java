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
package com.cfelde.rpqueue.test;

import com.cfelde.rpqueue.utils.CommonUtils;
import com.cfelde.rpqueue.utils.ImmutableByteArray;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author cfelde
 */
public class TestImmutableByteArray {
    
    public TestImmutableByteArray() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
    
    @Test
    public void toFromString() {
        String s = "This is my string";
        ImmutableByteArray iba = ImmutableByteArray.fromString(s);
        
        assertTrue(Arrays.equals(s.getBytes(CommonUtils.getCharset()), iba.getPayload()));
        assertEquals(s, ImmutableByteArray.toString(iba));
    }
    
    @Test
    public void toFromUUID1() {
        UUID uuid1 = UUID.randomUUID();
        
        ImmutableByteArray iba = ImmutableByteArray.fromUUID(uuid1);
        UUID uuid2 = ImmutableByteArray.toUUID(iba);
        
        assertEquals(uuid1, uuid2);
    }
    
    @Test
    public void toFromUUID2() {
        String s = "This is not a UUID";
        
        boolean gotException = false;
        
        try {
            ImmutableByteArray.toUUID(ImmutableByteArray.fromString(s));
        } catch (IllegalArgumentException ex) {
            gotException = true;
        }
        
        assertTrue(gotException);
    }
    
    @Test
    public void toFromLong1() {
        Random random = new Random();
        for (int i = 0; i < 1000000; i++) {
            long l = random.nextLong();
            assertEquals(l, ImmutableByteArray.toLong(ImmutableByteArray.fromLong(l)));
        }
    }
    
    @Test
    public void toFromLong2() {
        String s = "This is not a long";
        
        boolean gotException = false;
        
        try {
            ImmutableByteArray.toLong(ImmutableByteArray.fromString(s));
        } catch (IllegalArgumentException ex) {
            gotException = true;
        }
        
        assertTrue(gotException);
    }
    
    @Test
    public void toFromObject() {
        Long value = new Random().nextLong();
        
        boolean gotException = false;
        
        try {
            ImmutableByteArray iba = ImmutableByteArray.fromObject(value);
            Long out = ImmutableByteArray.toObject(iba);
            
            assertTrue(out instanceof Long);
            assertEquals((long)value, (long)out);
        } catch (Exception ex) {
            gotException = true;
        }
        
        assertFalse(gotException);
    }
}