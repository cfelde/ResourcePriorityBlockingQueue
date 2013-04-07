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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author cfelde
 */
public final class Task implements Comparable<Task> {
    public static enum STATUS {
        IDLE, QUEUED, ASSIGNED, CANCELED
    };
    
    private static final AtomicLong SEQ_GENERATOR = new AtomicLong();
    private final long sequence = SEQ_GENERATOR.getAndIncrement();
    
    private final ImmutableByteArray id;
    private final ImmutableByteArray payload;
    private final TaskGroup group;
    private final int priority;
    
    private final AtomicReference<STATUS> status = new AtomicReference<>(STATUS.IDLE);
    
    private ResourcePriorityBlockingQueue queue;

    /**
     * Create new task associated with given id and payload.
     *
     * Task will have a default priority of zero and will not be associated with
     * any task group.
     *
     * Neither id nor payload may be null or empty.
     *
     * @param id
     * @param payload
     */
    public Task(byte[] id, byte[] payload) {
        this(id, payload, 0, null);
    }

    /**
     * Create new task associated with given id, payload and priority.
     *
     * Task will not be associated with any group.
     *
     * Neither id nor payload may be null or empty.
     *
     * @param id
     * @param payload
     * @param priority
     */
    public Task(byte[] id, byte[] payload, int priority) {
        this(id, payload, priority, null);
    }

    /**
     * Create new task associated with given id, payload and group.
     *
     * Task will have a default priority of zero.
     *
     * Neither id nor payload may be null or empty.
     *
     * @param id
     * @param payload
     * @param group
     */
    public Task(byte[] id, byte[] payload, TaskGroup group) {
        this(id, payload, 0, group);
    }

    /**
     * Create new task associated with given id, payload, priority and group.
     *
     * Neither id nor payload may be null or empty.
     *
     * @param id
     * @param payload
     * @param priority
     * @param group
     */
    public Task(byte[] id, byte[] payload, int priority, TaskGroup group) {
        this(new ImmutableByteArray(id), new ImmutableByteArray(payload), priority, group);
    }

    /**
     * Create new task associated with given id and payload.
     *
     * Task will have a default priority of zero and will not be associated with
     * any task group.
     *
     * Neither id nor payload may be null or empty.
     *
     * @param id
     * @param payload
     */
    public Task(ImmutableByteArray id, ImmutableByteArray payload) {
        this(id, payload, 0, null);
    }

    /**
     * Create new task associated with given id, payload and priority.
     *
     * Task will not be associated with any group.
     *
     * Neither id nor payload may be null or empty.
     *
     * @param id
     * @param payload
     * @param priority
     */
    public Task(ImmutableByteArray id, ImmutableByteArray payload, int priority) {
        this(id, payload, priority, null);
    }

    /**
     * Create new task associated with given id, payload and group.
     *
     * Task will have a default priority of zero.
     *
     * Neither id nor payload may be null or empty.
     *
     * @param id
     * @param payload
     * @param group
     */
    public Task(ImmutableByteArray id, ImmutableByteArray payload, TaskGroup group) {
        this(id, payload, 0, group);
    }

    /**
     * Create new task associated with given id, payload, priority and group.
     *
     * Neither id nor payload may be null or empty.
     *
     * @param id
     * @param payload
     * @param priority
     * @param group
     */
    public Task(ImmutableByteArray id, ImmutableByteArray payload, int priority, TaskGroup group) {
        this.id = id;
        this.payload = payload;
        this.priority = priority;
        this.group = group == null ? TaskGroup.ZERO : group;
    }

    protected void setQueue(ResourcePriorityBlockingQueue queue) {
        if (status.compareAndSet(STATUS.IDLE, STATUS.QUEUED)) {
            this.queue = queue;
            this.group.addTask(this);
        } else {
            throw new IllegalStateException("Task not idle");
        }
    }

    public ImmutableByteArray getId() {
        return id;
    }

    public ImmutableByteArray getPayload() {
        return payload;
    }

    public int getPriority() {
        return priority;
    }

    public Task setPriority(int priority) {
        return setPriorityGroup(priority, group);
    }

    public TaskGroup getGroup() {
        return group;
    }

    public Task setGroup(TaskGroup group) {
        return setPriorityGroup(priority, group);
    }

    public Task setPriorityGroup(final int priority, final TaskGroup group) {
        switch (status.get()) {
            case IDLE:
                if (!status.compareAndSet(STATUS.IDLE, STATUS.CANCELED))
                    return setPriorityGroup(priority, group);

                return new Task(id, payload, priority, group);
                
            case ASSIGNED:
                return this;

            case CANCELED:
                return this;

            case QUEUED:
                if (!status.compareAndSet(STATUS.QUEUED, STATUS.CANCELED))
                    return setPriorityGroup(priority, group);
                
                queue.remove(this);

                Task newTask = new Task(id, payload, priority, group);
                
                if (!group.replaceTask(this, newTask) && !group.addTask(newTask))
                    throw new IllegalStateException("Failed to add updated task to group");
                
                queue.forceAdd(newTask);
                queue.clearTask(this);

                return newTask;

            default:
                throw new IllegalStateException("Unknown status: " + status);
        }
    }

    public STATUS getStatus() {
        return status.get();
    }

    protected boolean setStatus(STATUS expected, STATUS update) {
        return this.status.compareAndSet(expected, update);
    }
    
    public long getSequenceId() {
        return sequence;
    }

    @Override
    public int compareTo(Task otherTask) {
        // Order by group first (ascending)
        int cmp = (group.getPriority() < otherTask.group.getPriority()) ? -1 : ((group.getPriority() == otherTask.group.getPriority()) ? 0 : 1);

        if (cmp != 0) {
            return cmp;
        }

        // Other by task priority next (ascending)
        cmp = (priority < otherTask.priority) ? -1 : ((priority == otherTask.priority) ? 0 : 1);

        if (cmp != 0) {
            return cmp;
        }

        // Finally, order by sequence number (descending)
        return (otherTask.sequence < sequence) ? -1 : ((otherTask.sequence == sequence) ? 0 : 1);
    }
}
