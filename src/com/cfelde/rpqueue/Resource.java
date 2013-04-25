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

import com.cfelde.rpqueue.utils.ImmutableByteArray;
import java.util.concurrent.BlockingQueue;

/**
 * @author cfelde
 */
public final class Resource {
    private final ImmutableByteArray id;
    private final Object meta;
    
    // Holds a reference to the ResourcePriorityBlockingQueue on which this
    // resource is used. This correctly implies that a particular resource
    // instance can only be used against one queue.
    private BlockingQueue<Task> queue;
    
    /**
     * Create a new resource instance, associated with given ID.
     * 
     * The meta object can be any object type, and would typically be
     * useful only for {@link ResourceAllocator} and
     * {@link ResourcePrioritizer}.
     * 
     * @param id Resource ID
     * @param meta Resource meta object
     */
    public Resource(byte[] id, Object meta) {
        this(new ImmutableByteArray(id), meta);
    }
    
    /**
     * Create a new resource instance, associated with given ID.
     * 
     * The meta object can be any object type, and would typically be
     * useful only for {@link ResourceAllocator} and
     * {@link ResourcePrioritizer}.
     * 
     * @param id Resource ID
     * @param meta Resource meta object
     */
    public Resource(ImmutableByteArray id, Object meta) {
        this.id = id;
        this.meta = meta;
    }

    /**
     * Returns resource ID.
     * 
     * @return Resource ID
     */
    public ImmutableByteArray getId() {
        return id;
    }

    /**
     * Returns resource meta object.
     * 
     * @return Resource meta object
     */
    public Object getMeta() {
        return meta;
    }
    
    protected void setAssociatedQueue(BlockingQueue<Task> queue) {
        if (this.queue != null && this.queue != queue)
            throw new IllegalStateException();
        
        this.queue = queue;
    }
    
    protected BlockingQueue<Task> getAssociatedQueue() {
        return queue;
    }

    /**
     * Returns the size of the subscriber queue tied to this resource.
     * 
     * Method will throw a {@link IllegalStateException} is resource
     * isn't yet tied to a queue.
     * 
     * @return Number of tasks in queue tied to resource
     */
    public int getResourceQueueSize() {
        if (queue == null)
            throw new IllegalStateException();
        
        return queue.size();
    }

    /**
     * Returns true if queue tied to resource contains task.
     * 
     * @param task Task
     * @return True if resource queue contains task
     */
    public boolean isTaskAssignedToResource(Task task) {
        return queue.contains(task);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Resource other = (Resource) obj;
        
        return id.equals(other.id);
    }
}
