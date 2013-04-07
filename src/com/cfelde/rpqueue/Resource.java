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
    
    private BlockingQueue<Task> queue;
    
    public Resource(byte[] id, Object meta) {
        this(new ImmutableByteArray(id), meta);
    }
    
    public Resource(ImmutableByteArray id, Object meta) {
        this.id = id;
        this.meta = meta;
    }

    public ImmutableByteArray getId() {
        return id;
    }

    public Object getMeta() {
        return meta;
    }
    
    protected void setAssociatedQueue(BlockingQueue<Task> queue) {
        this.queue = queue;
    }
    
    protected BlockingQueue<Task> getAssociatedQueue() {
        return queue;
    }
    
    public int getResourceQueueSize() {
        return queue.size();
    }
    
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
