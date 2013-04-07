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
import com.cfelde.rpqueue.utils.ImmutableByteArray;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author cfelde
 */
public class PriorityBlockingQueueCompare1 {
    public static void main(String... args) throws Exception {
        ResourcePriorityBlockingQueue.createQueue("queue1", new FixedPrioritizer(), new AllAllocator());
        
        final ResourcePriorityBlockingQueue queue1 = ResourcePriorityBlockingQueue.getSubscriberQueue("queue1", new Resource(ImmutableByteArray.fromUUID(UUID.randomUUID()), null));
        final int size = 1000000;
        
        Thread producer1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < size; i++) {
                        queue1.offer(new Task(ImmutableByteArray.fromUUID(UUID.randomUUID()), ImmutableByteArray.fromUUID(UUID.randomUUID())), Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    }
                } catch (Exception ex) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        
        Thread consumer1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < size; i++) {
                        queue1.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    }
                } catch (Exception ex) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        
        long start = System.currentTimeMillis();
        producer1.start();
        consumer1.start();
        producer1.join();
        consumer1.join();
        long end = System.currentTimeMillis();
        
        System.out.println("ResourcePriorityBlockingQueue time: " + (end-start));
        
        final PriorityBlockingQueue<Task> queue2 = new PriorityBlockingQueue<Task>(Short.MAX_VALUE);
        
        Thread producer2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < size; i++) {
                        queue2.offer(new Task(ImmutableByteArray.fromUUID(UUID.randomUUID()), ImmutableByteArray.fromUUID(UUID.randomUUID())), Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    }
                } catch (Exception ex) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        
        Thread consumer2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < size; i++) {
                        queue2.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    }
                } catch (Exception ex) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        
        start = System.currentTimeMillis();
        producer2.start();
        consumer2.start();
        producer2.join();
        consumer2.join();
        end = System.currentTimeMillis();
        
        System.out.println("PriorityBlockingQueue time: " + (end-start));
    }
}
