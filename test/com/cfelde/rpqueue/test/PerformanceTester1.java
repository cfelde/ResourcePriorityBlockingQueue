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
import com.cfelde.rpqueue.schedulers.HighestPriorityAllocator;
import com.cfelde.rpqueue.schedulers.LeastBusyResourcePrioritizer;
import com.cfelde.rpqueue.utils.ImmutableByteArray;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author cfelde
 */
public class PerformanceTester1 implements Runnable {
    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final AtomicInteger consumedTasks = new AtomicInteger();
    
    private final int clientId;
    private final ResourcePriorityBlockingQueue queue;
    
    public static void main(String... args) throws InterruptedException {
        final int clients = Runtime.getRuntime().availableProcessors();
        final int tasks = 100000;
        
        ResourcePriorityBlockingQueue.createQueue("queue", new LeastBusyResourcePrioritizer(), new HighestPriorityAllocator());
        //ResourcePriorityBlockingQueue.createQueue("queue", new FixedPrioritizer(), new AllAllocator());
        
        System.out.println("Starting " + clients + " clients..");
        List<Thread> threads = new ArrayList<Thread>();
        for (int client = 0; client < clients; client++) {
            Thread t = new Thread(
                    new PerformanceTester1(client,
                    ResourcePriorityBlockingQueue.getSubscriberQueue("queue", 
                    new Resource(ImmutableByteArray.fromLong(client), null)))
            );
            
            t.start();
            
            threads.add(t);
        }
        
        final ResourcePriorityBlockingQueue queue = ResourcePriorityBlockingQueue.getPublisherQueue("queue");
        
        System.out.println("Starting 1 master writer offering " + tasks + " tasks");
        final long start = System.currentTimeMillis();
        for (int task = 0; task < tasks; task++) {
            queue.offer(new Task(ImmutableByteArray.fromLong(task), ImmutableByteArray.fromUUID(UUID.randomUUID())), Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
        
        while (!queue.isEmpty())
            Thread.sleep(1);
        final long end = System.currentTimeMillis();
        
        System.out.println("Offering " + tasks + " took " + (end - start) + " ms");
        running.set(false);
        
        for (Thread t: threads)
            t.join();
        
        System.out.println("Total consumed tasks: " + consumedTasks.get());
    }
    
    public PerformanceTester1(int clientId, ResourcePriorityBlockingQueue queue) {
        this.clientId = clientId;
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            int polled = 0;
            while (running.get()) {
                if (queue.poll(1000, TimeUnit.MILLISECONDS) != null)
                    polled++;
                
                // Use this to add some artificial client processing time.
                // Adding this artificial load will make the distribution
                // of tasks among client more even (depending of course on
                // the allocator used)
                //if (polled % 1 == 0)
                //    Thread.sleep(1);
            }
            
            System.out.println("Client id = " + clientId + " polled " + polled + " tasks");
            consumedTasks.addAndGet(polled);
        } catch (InterruptedException ex) {
            running.set(false);
            Thread.currentThread().interrupt();
        }
    }
}
