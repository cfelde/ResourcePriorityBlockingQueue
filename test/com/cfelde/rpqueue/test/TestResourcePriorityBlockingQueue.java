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

import com.cfelde.rpqueue.Resource;
import com.cfelde.rpqueue.ResourcePriorityBlockingQueue;
import com.cfelde.rpqueue.Task;
import com.cfelde.rpqueue.schedulers.AllAllocator;
import com.cfelde.rpqueue.schedulers.FixedPrioritizer;
import com.cfelde.rpqueue.schedulers.PayloadAllocator;
import com.cfelde.rpqueue.utils.ImmutableByteArray;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author cfelde
 */
public class TestResourcePriorityBlockingQueue {
    
    public TestResourcePriorityBlockingQueue() {
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
        for (String name: ResourcePriorityBlockingQueue.getAllQueues().keySet().toArray(new String[0])) {
            ResourcePriorityBlockingQueue.shutdownQueue(name);
        }
    }
    
    @Test
    public void ensureUniqueQueue() {
        assertTrue(ResourcePriorityBlockingQueue.createQueue("ensureUniqueQueue1", new FixedPrioritizer(), new AllAllocator()));
        assertFalse(ResourcePriorityBlockingQueue.createQueue("ensureUniqueQueue1", new FixedPrioritizer(), new AllAllocator()));
    }
    
    @Test
    public void singlePubSingleSub() {
        assertTrue(ResourcePriorityBlockingQueue.createQueue("q1", new FixedPrioritizer(), new AllAllocator(), 1000));
        
        ResourcePriorityBlockingQueue pub = ResourcePriorityBlockingQueue.getPublisherQueue("q1");
        ResourcePriorityBlockingQueue sub = ResourcePriorityBlockingQueue.getSubscriberQueue("q1", new Resource(ImmutableByteArray.fromString("sub1"), null));
        
        assertTrue(pub.isEmpty());
        assertTrue(sub.isEmpty());
        
        Task task = new Task(ImmutableByteArray.fromString("task1"), ImmutableByteArray.fromString("payload1"));
        assertTrue(pub.offer(task));
        
        assertEquals(1, pub.size());
        assertEquals(1, sub.size());
        
        assertEquals(task, sub.peek());
        assertEquals(task, sub.poll());
        assertNull(sub.peek());
        assertNull(sub.poll());
        assertTrue(pub.isEmpty());
        assertTrue(sub.isEmpty());
    }
    
    @Test
    public void singlePubTwoSub() {
        assertTrue(ResourcePriorityBlockingQueue.createQueue("q1", new FixedPrioritizer(), new AllAllocator(), 1000));
        
        ResourcePriorityBlockingQueue pub = ResourcePriorityBlockingQueue.getPublisherQueue("q1");
        ResourcePriorityBlockingQueue sub1 = ResourcePriorityBlockingQueue.getSubscriberQueue("q1", new Resource(ImmutableByteArray.fromString("sub1"), null));
        ResourcePriorityBlockingQueue sub2 = ResourcePriorityBlockingQueue.getSubscriberQueue("q1", new Resource(ImmutableByteArray.fromString("sub2"), null));
        
        assertTrue(pub.isEmpty());
        assertTrue(sub1.isEmpty());
        assertTrue(sub2.isEmpty());
        
        Task task = new Task(ImmutableByteArray.fromString("task1"), ImmutableByteArray.fromString("payload1"));
        assertTrue(pub.offer(task));
        
        assertEquals(1, pub.size());
        assertEquals(1, sub1.size());
        assertEquals(1, sub2.size());
        
        assertEquals(task, sub1.peek());
        assertEquals(task, sub2.peek());
        assertEquals(task, sub1.poll());
        assertNull(sub2.poll());
        
        assertNull(sub1.peek());
        assertNull(sub1.poll());
        assertTrue(pub.isEmpty());
        assertTrue(sub1.isEmpty());
        assertTrue(sub2.isEmpty());
    }
    
    @Test
    public void singlePubTwoDedicatedSub() {
        ImmutableByteArray payload1 = ImmutableByteArray.fromString("payload1");
        ImmutableByteArray payload2 = ImmutableByteArray.fromString("payload2");
        
        Resource resource1 = new Resource(ImmutableByteArray.fromString("resource1"), payload1);
        Resource resource2 = new Resource(ImmutableByteArray.fromString("resource2"), payload2);
        
        assertTrue(ResourcePriorityBlockingQueue.createQueue("q1", new FixedPrioritizer(), new PayloadAllocator(), 1000));
        
        ResourcePriorityBlockingQueue pub = ResourcePriorityBlockingQueue.getPublisherQueue("q1");
        ResourcePriorityBlockingQueue sub1 = ResourcePriorityBlockingQueue.getSubscriberQueue("q1", resource1);
        ResourcePriorityBlockingQueue sub2 = ResourcePriorityBlockingQueue.getSubscriberQueue("q1", resource2);
        
        assertTrue(pub.isEmpty());
        assertTrue(sub1.isEmpty());
        assertTrue(sub2.isEmpty());
        
        Task task1 = new Task(ImmutableByteArray.fromString("task1"), payload1);
        assertTrue(pub.offer(task1));
        
        assertEquals(1, pub.size());
        assertEquals(1, sub1.size());
        assertEquals(0, sub2.size());
        
        Task task2 = new Task(ImmutableByteArray.fromString("task2"), payload2);
        assertTrue(pub.offer(task2));
        
        assertEquals(2, pub.size());
        assertEquals(1, sub1.size());
        assertEquals(1, sub2.size());
        
        assertEquals(task1, sub1.peek());
        assertEquals(task2, sub2.peek());
        assertEquals(2, pub.size());
        assertEquals(task1, sub1.poll());
        assertEquals(1, pub.size());
        assertEquals(task2, sub2.poll());
        assertEquals(0, pub.size());
        
        assertNull(sub1.peek());
        assertNull(sub1.poll());
        assertNull(sub2.peek());
        assertNull(sub2.poll());
        
        assertTrue(pub.isEmpty());
        assertTrue(sub1.isEmpty());
        assertTrue(sub2.isEmpty());
    }
    
    @Test
    public void maxLimitBlock1() throws InterruptedException {
        final int maxSize = 100;
        
        assertTrue(ResourcePriorityBlockingQueue.createQueue("q1", new FixedPrioritizer(), new AllAllocator(), maxSize));
        
        final ResourcePriorityBlockingQueue queue = ResourcePriorityBlockingQueue.getSubscriberQueue("q1", new Resource(ImmutableByteArray.fromString("r1"), null));
        
        assertEquals(0, queue.size());
        assertEquals(maxSize, queue.remainingCapacity());
        
        for (int i = 0; i < maxSize; i++) {
            assertTrue(queue.offer(new Task(ImmutableByteArray.fromLong(i), ImmutableByteArray.fromUUID(UUID.randomUUID()))));
        }
        
        assertFalse(queue.offer(new Task(ImmutableByteArray.fromLong(100), ImmutableByteArray.fromUUID(UUID.randomUUID()))));
        
        assertEquals(maxSize, queue.size());
        assertEquals(0, queue.remainingCapacity());
        
        final AtomicBoolean didOffer = new AtomicBoolean();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    queue.offer(new Task(ImmutableByteArray.fromLong(100), ImmutableByteArray.fromUUID(UUID.randomUUID())), Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    didOffer.set(true);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        t.start();
        
        Thread.sleep(1000);
        
        assertTrue(t.isAlive());
        assertFalse(didOffer.get());
        assertNotNull(queue.poll());
        
        t.join();
        
        assertTrue(didOffer.get());
        assertEquals(maxSize, queue.size());
        assertEquals(0, queue.remainingCapacity());
    }
    
    @Test
    public void maxLimitBlock2() throws InterruptedException {
        final int maxSize = 100;
        
        assertTrue(ResourcePriorityBlockingQueue.createQueue("q1", new FixedPrioritizer(), new AllAllocator(), maxSize));
        
        final ResourcePriorityBlockingQueue queue = ResourcePriorityBlockingQueue.getSubscriberQueue("q1", new Resource(ImmutableByteArray.fromString("r1"), null));
        
        assertEquals(0, queue.size());
        assertEquals(maxSize, queue.remainingCapacity());
        
        for (int i = 0; i < maxSize; i++) {
            assertTrue(queue.offer(new Task(ImmutableByteArray.fromLong(i), ImmutableByteArray.fromUUID(UUID.randomUUID()))));
        }
        
        assertFalse(queue.offer(new Task(ImmutableByteArray.fromLong(100), ImmutableByteArray.fromUUID(UUID.randomUUID()))));
        
        assertEquals(maxSize, queue.size());
        assertEquals(0, queue.remainingCapacity());
        
        final AtomicBoolean didOffer = new AtomicBoolean();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    queue.offer(new Task(ImmutableByteArray.fromLong(100), ImmutableByteArray.fromUUID(UUID.randomUUID())), Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    didOffer.set(true);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        t.start();
        
        Thread.sleep(1000);
        
        assertTrue(t.isAlive());
        assertFalse(didOffer.get());
        assertTrue(queue.remove(queue.peek()));
        
        t.join();
        
        assertTrue(didOffer.get());
        assertEquals(maxSize, queue.size());
        assertEquals(0, queue.remainingCapacity());
    }
    
    @Test
    public void drainTo1() {
        final int maxSize = 1000;
        
        assertTrue(ResourcePriorityBlockingQueue.createQueue("q1", new FixedPrioritizer(), new AllAllocator()));
        
        final ResourcePriorityBlockingQueue queue = ResourcePriorityBlockingQueue.getSubscriberQueue("q1", new Resource(ImmutableByteArray.fromString("r1"), null));
        
        for (int i = 0; i < maxSize; i++) {
            assertTrue(queue.offer(new Task(ImmutableByteArray.fromLong(i), ImmutableByteArray.fromUUID(UUID.randomUUID()))));
        }
        
        List<Task> tasks = new ArrayList<Task>();
        queue.drainTo(tasks);
        
        assertEquals(maxSize, tasks.size());
        assertTrue(queue.isEmpty());
    }
    
    @Test
    public void drainTo2() {
        final int maxSize = 1000;
        
        assertTrue(ResourcePriorityBlockingQueue.createQueue("q1", new FixedPrioritizer(), new AllAllocator()));
        
        final ResourcePriorityBlockingQueue queue1 = ResourcePriorityBlockingQueue.getPublisherQueue("q1");
        final ResourcePriorityBlockingQueue queue2 = ResourcePriorityBlockingQueue.getSubscriberQueue("q1", new Resource(ImmutableByteArray.fromString("r1"), null));
        
        for (int i = 0; i < maxSize; i++) {
            assertTrue(queue1.offer(new Task(ImmutableByteArray.fromLong(i), ImmutableByteArray.fromUUID(UUID.randomUUID()))));
        }
        
        final AtomicBoolean gotException = new AtomicBoolean();
        
        try {
            queue1.drainTo(queue1);
        } catch (IllegalArgumentException ex) {
            gotException.set(true);
        }
        
        assertTrue(gotException.get());
        
        gotException.set(false);
        
        try {
            queue1.drainTo(queue2);
        } catch (IllegalArgumentException ex) {
            gotException.set(true);
        }
        
        assertTrue(gotException.get());
        
        gotException.set(false);
        
        try {
            queue2.drainTo(queue1);
        } catch (IllegalArgumentException ex) {
            gotException.set(true);
        }
        
        assertTrue(gotException.get());
        
        gotException.set(false);
        
        try {
            queue2.drainTo(queue2);
        } catch (IllegalArgumentException ex) {
            gotException.set(true);
        }
        
        assertTrue(gotException.get());
        
        assertFalse(queue1.isEmpty());
        assertFalse(queue2.isEmpty());
        assertEquals(maxSize, queue1.size());
        assertEquals(maxSize, queue2.size());
    }
    
    @Test
    public void iterator() {
        final int maxSize = 1000;
        
        assertTrue(ResourcePriorityBlockingQueue.createQueue("q1", new FixedPrioritizer(), new AllAllocator()));
        
        final ResourcePriorityBlockingQueue queue1 = ResourcePriorityBlockingQueue.getPublisherQueue("q1");
        final ResourcePriorityBlockingQueue queue2 = ResourcePriorityBlockingQueue.getSubscriberQueue("q1", new Resource(ImmutableByteArray.fromString("r1"), null));
        
        for (int i = 0; i < maxSize; i++) {
            assertTrue(queue1.offer(new Task(ImmutableByteArray.fromLong(i), ImmutableByteArray.fromUUID(UUID.randomUUID()))));
        }
        
        Iterator<Task> it1 = queue1.iterator();
        for (int i = 0; i < maxSize; i++) {
            assertTrue(it1.hasNext());
            assertNotNull(it1.next());
        }
        
        assertFalse(it1.hasNext());
        
        Iterator<Task> it2 = queue2.iterator();
        for (int i = 0; i < maxSize; i++) {
            assertTrue(it2.hasNext());
            assertNotNull(it2.next());
        }
        
        assertFalse(it2.hasNext());
    }
}