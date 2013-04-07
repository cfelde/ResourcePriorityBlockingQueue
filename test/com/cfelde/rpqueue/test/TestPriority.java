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
import com.cfelde.rpqueue.TaskGroup;
import com.cfelde.rpqueue.schedulers.AllAllocator;
import com.cfelde.rpqueue.schedulers.FixedPrioritizer;
import com.cfelde.rpqueue.utils.ImmutableByteArray;
import java.util.Collection;
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
public class TestPriority {
    
    public TestPriority() {
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
    public void taskPriority1() {
        assertTrue(ResourcePriorityBlockingQueue.createQueue("queue", new FixedPrioritizer(), new AllAllocator()));
        
        ResourcePriorityBlockingQueue queue = ResourcePriorityBlockingQueue.getSubscriberQueue("queue", new Resource(ImmutableByteArray.fromLong(1), null));
        
        Task task1 = new Task(ImmutableByteArray.fromUUID(UUID.randomUUID()), ImmutableByteArray.fromString("payload1"), 100);
        Task task2 = new Task(ImmutableByteArray.fromUUID(UUID.randomUUID()), ImmutableByteArray.fromString("payload2"), 200);
        
        assertTrue(queue.offer(task1));
        assertEquals(task1, queue.peek());
        
        assertTrue(queue.offer(task2));
        assertEquals(task2, queue.peek());
        assertEquals(2, queue.size());
        
        assertEquals(task2, queue.poll());
        assertEquals(task1, queue.poll());
        
        assertTrue(queue.isEmpty());
    }
    
    @Test
    public void taskPriority2() {
        assertTrue(ResourcePriorityBlockingQueue.createQueue("queue", new FixedPrioritizer(), new AllAllocator()));
        
        ResourcePriorityBlockingQueue queue = ResourcePriorityBlockingQueue.getSubscriberQueue("queue", new Resource(ImmutableByteArray.fromLong(1), null));
        
        Task task1 = new Task(ImmutableByteArray.fromUUID(UUID.randomUUID()), ImmutableByteArray.fromString("payload1"), 100);
        Task task2 = new Task(ImmutableByteArray.fromUUID(UUID.randomUUID()), ImmutableByteArray.fromString("payload2"), 200);
        
        assertTrue(queue.offer(task1));
        assertEquals(task1, queue.peek());
        
        assertTrue(queue.offer(task2));
        assertEquals(task2, queue.peek());
        assertEquals(2, queue.size());
        
        Task newTask2 = task2.setPriority(50);
        
        assertEquals(Task.STATUS.CANCELED, task2.getStatus());
        
        assertEquals(task1, queue.peek());
        
        assertEquals(task1, queue.poll());
        assertEquals(newTask2, queue.poll());
        
        assertTrue(queue.isEmpty());
    }
    
    @Test
    public void groupPriority1() {
        assertTrue(ResourcePriorityBlockingQueue.createQueue("queue", new FixedPrioritizer(), new AllAllocator()));
        
        ResourcePriorityBlockingQueue queue = ResourcePriorityBlockingQueue.getSubscriberQueue("queue", new Resource(ImmutableByteArray.fromLong(1), null));
        
        TaskGroup group1 = TaskGroup.createGroup(ImmutableByteArray.fromUUID(UUID.randomUUID()), 1000);
        TaskGroup group2 = TaskGroup.createGroup(ImmutableByteArray.fromUUID(UUID.randomUUID()), 2000);
        
        Task task1 = new Task(ImmutableByteArray.fromUUID(UUID.randomUUID()), ImmutableByteArray.fromString("payload1"), group1);
        Task task2 = new Task(ImmutableByteArray.fromUUID(UUID.randomUUID()), ImmutableByteArray.fromString("payload2"), group2);
        
        assertTrue(queue.offer(task1));
        assertEquals(task1, queue.peek());
        
        assertTrue(queue.offer(task2));
        assertEquals(task2, queue.peek());
        assertEquals(2, queue.size());
        
        assertEquals(task2, queue.poll());
        assertEquals(task1, queue.poll());
        
        assertTrue(queue.isEmpty());
    }
    
    @Test
    public void groupPriority2() {
        assertTrue(ResourcePriorityBlockingQueue.createQueue("queue", new FixedPrioritizer(), new AllAllocator()));
        
        ResourcePriorityBlockingQueue queue = ResourcePriorityBlockingQueue.getSubscriberQueue("queue", new Resource(ImmutableByteArray.fromLong(1), null));
        
        TaskGroup group1 = TaskGroup.createGroup(ImmutableByteArray.fromUUID(UUID.randomUUID()), 1000);
        TaskGroup group2 = TaskGroup.createGroup(ImmutableByteArray.fromUUID(UUID.randomUUID()), 2000);
        
        Task task1 = new Task(ImmutableByteArray.fromUUID(UUID.randomUUID()), ImmutableByteArray.fromString("payload1"), group1);
        Task task2 = new Task(ImmutableByteArray.fromUUID(UUID.randomUUID()), ImmutableByteArray.fromString("payload2"), group2);
        
        assertTrue(queue.offer(task1));
        assertEquals(task1, queue.peek());
        
        assertTrue(queue.offer(task2));
        assertEquals(task2, queue.peek());
        assertEquals(2, queue.size());
        
        assertEquals(1, group1.size());
        Collection<Task> groupTasks = group1.setPriority(3000);
        assertEquals(1, group1.size());
        assertEquals(1, groupTasks.size());
        Task newTask1 = groupTasks.iterator().next();
        
        assertEquals(newTask1, queue.poll());
        assertEquals(task2, queue.poll());
        
        assertTrue(queue.isEmpty());
    }
}