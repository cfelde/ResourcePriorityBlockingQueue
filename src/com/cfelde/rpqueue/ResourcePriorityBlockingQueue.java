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
package com.cfelde.rpqueue;

import com.cfelde.rpqueue.utils.SortedInteger;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author cfelde
 */
public class ResourcePriorityBlockingQueue implements BlockingQueue<Task>, Closeable {
    private static final ConcurrentHashMap<String, ResourcePriorityBlockingQueue> queues = new ConcurrentHashMap<String, ResourcePriorityBlockingQueue>();
    
    private final ResourcePrioritizer prioritizer;
    private final ResourceAllocator allocator;
    private final int maxSize;
    
    private final ConcurrentSkipListMap<Task, Set<Resource>> tasks;
    private final ConcurrentHashMap<Resource, ResourcePriorityBlockingQueue> resources;
    
    private final ResourcePriorityBlockingQueue parentQueue;
    private final PriorityBlockingQueue<Task> subscriberQueue;
    private final Resource resource;
    
    private final Object tasksNotification;
    
    private volatile boolean isActive = true;
    
    private ResourcePriorityBlockingQueue(ResourcePrioritizer prioritizer, ResourceAllocator allocator, int maxSize) {
        this.prioritizer = prioritizer;
        this.allocator = allocator;
        this.maxSize = maxSize;
        this.tasks = new ConcurrentSkipListMap<Task, Set<Resource>>(Collections.reverseOrder());
        this.resources = new ConcurrentHashMap<Resource, ResourcePriorityBlockingQueue>();
        
        this.parentQueue = null;
        this.subscriberQueue = null;
        this.resource = null;
        
        this.tasksNotification = new Object();
    }
    
    private ResourcePriorityBlockingQueue(ResourcePriorityBlockingQueue parentQueue, Resource resource) {
        this.prioritizer = parentQueue.prioritizer;
        this.allocator = parentQueue.allocator;
        this.maxSize = parentQueue.maxSize;
        this.tasks = parentQueue.tasks;
        this.resources = parentQueue.resources;
        
        this.parentQueue = parentQueue;
        this.subscriberQueue = new PriorityBlockingQueue<Task>(parentQueue.maxSize > Short.MAX_VALUE ? Short.MAX_VALUE : parentQueue.maxSize, Collections.reverseOrder());
        this.resource = resource;
        
        this.tasksNotification = parentQueue.tasksNotification;
    }
    
    public static boolean createQueue(String name, ResourcePrioritizer prioritizer, ResourceAllocator allocator) {
        return createQueue(name, prioritizer, allocator, Integer.MAX_VALUE);
    }
    
    public static boolean createQueue(String name, ResourcePrioritizer prioritizer, ResourceAllocator allocator, int maxSize) {
        return queues.putIfAbsent(name, new ResourcePriorityBlockingQueue(prioritizer, allocator, maxSize)) == null;
    }
    
    public static Map<String, ResourcePriorityBlockingQueue> getAllQueues() {
        return Collections.unmodifiableMap(queues);
    }
    
    public static boolean shutdownQueue(String name) {
        ResourcePriorityBlockingQueue removedQueue = queues.remove(name);
        
        if (removedQueue == null)
            return false;
        
        removedQueue.isActive = false;
        
        for (Task task: removedQueue.tasks.keySet().toArray(new Task[0])) {
            removedQueue.clearTask(task);
        }
        
        removedQueue.tasks.clear();
        removedQueue.resources.clear();
        
        return true;
    }
    
    public static ResourcePriorityBlockingQueue getPublisherQueue(String name) {
        return queues.get(name);
    }
    
    public static ResourcePriorityBlockingQueue getSubscriberQueue(String name, Resource resource) {
        ResourcePriorityBlockingQueue parentQueue = getPublisherQueue(name);
        
        if (parentQueue == null)
            return null;
        
        ResourcePriorityBlockingQueue subscriberQueue = new ResourcePriorityBlockingQueue(parentQueue, resource);
        subscriberQueue = parentQueue.resources.putIfAbsent(resource, subscriberQueue);
        
        if (subscriberQueue != null)
            return subscriberQueue;
        
        subscriberQueue = parentQueue.resources.get(resource);
        resource.setAssociatedQueue(subscriberQueue.subscriberQueue);
        
        Set<Resource> resourceSet = Collections.singleton(resource);
        for (Task task: parentQueue.tasks.keySet()) {
            parentQueue.allocateTask(task, resourceSet);
        }
        
        return subscriberQueue;
    }
    
    protected void allocateTask(Task task, Set<Resource> resources) {
        SortedMap<SortedInteger, Resource> sortedResources = new TreeMap<SortedInteger, Resource>(Collections.reverseOrder());
        
        Set<Resource> allResources = Collections.unmodifiableSet(this.resources.keySet());
        
        for (Resource resource: resources) {
            Integer score = prioritizer.evaluate(task, resource, allResources);
            
            if (score == null)
                continue;
            
            sortedResources.put(new SortedInteger(score), resource);
        }
        
        for (Resource resource: allocator.assign(task, sortedResources, allResources)) {
            if (!sortedResources.containsValue(resource))
                throw new IllegalArgumentException("Resource not available for assignement");
                
            tasks.get(task).add(resource);
            resource.getAssociatedQueue().offer(task);
        }
    }
    
    protected void clearTask(Task task) {
        if (tasks.remove(task) != null) {
            for (Resource resource: resources.keySet()) {
                resource.getAssociatedQueue().remove(task);
            }
            
            task.getGroup().removeTask(task);
            
            synchronized (tasksNotification) {
                tasksNotification.notifyAll();
            }
        }
    }
    
    protected void forceAdd(Task task) {
        ensureTaskState(task);
        
        if (isActive && tasks.putIfAbsent(task, Collections.newSetFromMap(new ConcurrentHashMap<Resource, Boolean>())) == null) {
            allocateTask(task, resources.keySet());
            
            synchronized (tasksNotification) {
                tasksNotification.notifyAll();
            }
        }
    }
    
    private void ensureTaskState(Task task) {
        if (isSubscriberQueue())
            task.setQueue(parentQueue);
        else
            task.setQueue(this);
    }
    
    public boolean isSubscriberQueue() {
        return subscriberQueue != null;
    }

    @Override
    public boolean add(Task task) {
        if (!offer(task))
            throw new IllegalStateException();
        
        return true;
    }

    @Override
    public boolean offer(Task task) {
        if (tasks.size() < maxSize && isActive) {
            forceAdd(task);
            
            return true;
        }
        
        return false;
    }

    @Override
    public void put(Task task) throws InterruptedException {
        offer(task, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean offer(Task task, long timeout, TimeUnit unit) throws InterruptedException {
        long duration = TimeUnit.MILLISECONDS.convert(timeout, unit);
        
        while (duration > 0) {
            long start = System.currentTimeMillis();
            
            if (offer(task))
                return true;
            
            synchronized (tasksNotification) {
                tasksNotification.wait(Math.min(1000, duration));
            }
            
            duration -= System.currentTimeMillis() - start;
        }
        
        return false;
    }

    @Override
    public Task take() throws InterruptedException {
        return poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public Task poll(long timeout, TimeUnit unit) throws InterruptedException {
        long duration = TimeUnit.MILLISECONDS.convert(timeout, unit);
        
        while (duration > 0) {
            long start = System.currentTimeMillis();
            
            Task task = null;
            
            if (isSubscriberQueue()) {
                task = subscriberQueue.poll(duration, TimeUnit.MILLISECONDS);
            } else {
                while (duration > 0) {
                    Map.Entry<Task, Set<Resource>> entry = tasks.firstEntry();
                    
                    if (entry == null) {
                        synchronized (tasksNotification) {
                            tasksNotification.wait(Math.min(1000, duration));
                        }
                    } else {
                        task = entry.getKey();
                        break;
                    }
                    
                    duration -= System.currentTimeMillis() - start;
                    start = System.currentTimeMillis();
                }
            }
            
            if (task == null)
                return null;
            
            if (task.setStatus(Task.STATUS.QUEUED, Task.STATUS.ASSIGNED)) {
                clearTask(task);
                
                return task;
            }
            
            duration -= System.currentTimeMillis() - start;
        }
        
        return null;
    }

    @Override
    public int remainingCapacity() {
        return Math.max(maxSize - tasks.size(), 0);
    }

    @Override
    public boolean remove(Object task) {
        if (task instanceof Task && contains((Task)task)) {
            Task t = (Task) task;
            
            if (t.setStatus(Task.STATUS.QUEUED, Task.STATUS.CANCELED)) {
                clearTask(t);
                
                return true;
            }
        }
        
        return false;
    }

    @Override
    public boolean contains(Object task) {
        if (task instanceof Task)
            return ((Task)task).getStatus() == Task.STATUS.QUEUED && tasks.containsKey((Task)task);
        else
            return false;
    }

    @Override
    public int drainTo(Collection<? super Task> collection) {
        return drainTo(collection, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super Task> collection, int maxElements) {
        if (collection instanceof ResourcePriorityBlockingQueue) {
            ResourcePriorityBlockingQueue otherQueue = (ResourcePriorityBlockingQueue) collection;
            
            ResourcePriorityBlockingQueue otherRootQueue = otherQueue.isSubscriberQueue() ? otherQueue.parentQueue : otherQueue;
            ResourcePriorityBlockingQueue thisRootQueue = isSubscriberQueue() ? parentQueue : this;
            
            if (otherRootQueue == thisRootQueue)
                throw new IllegalArgumentException();
        }
        
        int count = 0;
        if (isSubscriberQueue()) {
            Task task;
            while (count < maxElements && (task = poll()) != null) {
                collection.add(task);
                count++;
            }
        }
        
        return count;
    }

    @Override
    public Task remove() {
        Task task = poll();
        
        if (task == null)
            throw new NoSuchElementException();
        
        return task;
    }

    @Override
    public Task poll() {
        while (true) {
            Task task = null;
            
            if (isSubscriberQueue()) {
                task = subscriberQueue.poll();
            } else {
                Map.Entry<Task, Set<Resource>> entry = tasks.firstEntry();
                
                if (entry != null)
                    task = entry.getKey();
            }
            
            if (task == null)
                return null;
            
            if (task.setStatus(Task.STATUS.QUEUED, Task.STATUS.ASSIGNED)) {
                clearTask(task);
                
                return task;
            }
        }
    }

    @Override
    public Task element() {
        Task task = peek();
        
        if (task == null)
            throw new NoSuchElementException();
        
        return task;
    }

    @Override
    public Task peek() {
        if (isSubscriberQueue()) {
            return subscriberQueue.peek();
        } else {
            Map.Entry<Task, Set<Resource>> entry = tasks.firstEntry();
            
            if (entry == null)
                return null;
            
            return entry.getKey();
        }
    }

    @Override
    public int size() {
        if (isSubscriberQueue())
            return subscriberQueue.size();
        else
            return tasks.size();
    }

    @Override
    public boolean isEmpty() {
        if (isSubscriberQueue())
            return subscriberQueue.isEmpty();
        else
            return tasks.isEmpty();
    }

    @Override
    public Iterator<Task> iterator() {
        if (isSubscriberQueue()) {
            final Iterator<Task> it = subscriberQueue.iterator();
            
            return new Iterator<Task>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Task next() {
                    return it.next();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        } else {
            final Iterator<Task> it = tasks.keySet().iterator();
            
            return new Iterator<Task>() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Task next() {
                    return it.next();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    @Override
    public Object[] toArray() {
        if (isSubscriberQueue())
            return subscriberQueue.toArray();
        else
            return tasks.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        if (isSubscriberQueue())
            return subscriberQueue.toArray(ts);
        else
            return tasks.keySet().toArray(ts);
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        for (Object o: collection) {
            if (o instanceof Task) {
                if (!tasks.containsKey((Task)o))
                    return false;
            } else {
                return false;
            }
        }
        
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends Task> collection) {
        for (Task task: collection)
            add(task);
        
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        for (Object o: collection) {
            if (o instanceof Task)
                remove((Task)o);
        }
        
        return true;
    }

    @Override
    public boolean retainAll(Collection<?> clctn) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void clear() {
        // TODO This should probably close the tasks
        tasks.clear();
        
        for (Resource resource: resources.keySet())
            resource.getAssociatedQueue().clear();
        
        synchronized (tasksNotification) {
            tasksNotification.notifyAll();
        }
    }

    @Override
    public void close() throws IOException {
        if (!isSubscriberQueue() || resources.remove(resource) == null)
            return;
        
        // Find orphan tasks
        for (Map.Entry<Task, Set<Resource>> entry: tasks.entrySet()) {
            Task task = entry.getKey();
            Set<Resource> resources = entry.getValue();
            
            if (resources.remove(resource) && resources.isEmpty()) {
                allocateTask(task, this.resources.keySet());
            }
        }
    }
}
