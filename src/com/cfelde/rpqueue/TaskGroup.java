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
import com.cfelde.rpqueue.utils.CommonUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author cfelde
 */
public final class TaskGroup {
    private final ImmutableByteArray id;
    private final int priority;
    
    private final ConcurrentHashMap<ImmutableByteArray, Task> tasks = new ConcurrentHashMap<ImmutableByteArray, Task>();
    
    public static final ImmutableByteArray ZERO_ID = new ImmutableByteArray("ZERO".getBytes(CommonUtils.getCharset()));
    public static final TaskGroup ZERO = new TaskGroup(ZERO_ID, 0);
    
    private static final ConcurrentHashMap<ImmutableByteArray, TaskGroup> groups = new ConcurrentHashMap<ImmutableByteArray, TaskGroup>();
    
    static {
        groups.put(ZERO_ID, ZERO);
    }
    
    public static TaskGroup getGroup(byte[] id) {
        return getGroup(new ImmutableByteArray(id));
    }
    
    public static TaskGroup getGroup(ImmutableByteArray id) {
        return groups.get(id);
    }
    
    public static TaskGroup createGroup(byte[] id) {
        return createGroup(id, 0);
    }
    
    public static TaskGroup createGroup(ImmutableByteArray id) {
        return createGroup(id, 0);
    }
    
    public static TaskGroup createGroup(byte[] id, int priority) {
        return createGroup(new ImmutableByteArray(id), priority);
    }
    
    public static TaskGroup createGroup(ImmutableByteArray id, int priority) {
        groups.putIfAbsent(id, new TaskGroup(id, priority));
        
        return groups.get(id);
    }
    
    public static Map<ImmutableByteArray, TaskGroup> getAllGroups() {
        return Collections.unmodifiableMap(groups);
    }
    
    private TaskGroup(ImmutableByteArray id, int priority) {
        this.id = id;
        this.priority = priority;
    }
    
    protected boolean addTask(Task task) {
        return getGroup(id).tasks.putIfAbsent(task.getId(), task) == null;
    }
    
    protected synchronized boolean removeTask(Task task) {
        return getGroup(id).tasks.remove(task.getId(), task);
    }
    
    protected synchronized boolean replaceTask(Task oldTask, Task newTask) {
        return getGroup(id).tasks.replace(newTask.getId(), oldTask, newTask);
    }
    
    public boolean haveTask(Task task) {
        return getGroup(id).tasks.containsKey(task.getId());
    }
    
    public Collection<Task> getTasks() {
        return Collections.unmodifiableCollection(getGroup(id).tasks.values());
    }
    
    public int size() {
        return getGroup(id).tasks.size();
    }
    
    public synchronized Collection<Task> setPriority(int priority) {
        if (ZERO_ID.equals(id))
            throw new UnsupportedOperationException("Priority can not be changed on ZERO group");
        
        if (getGroup(id).priority == priority)
            return getTasks();
        
        TaskGroup newGroup = new TaskGroup(id, priority);
        TaskGroup oldGroup = groups.get(id);
        
        Collection<Task> oldTasks = oldGroup.getTasks();
        
        if (oldGroup != groups.put(id, newGroup))
            throw new ConcurrentModificationException();
        
        for (Task task: oldTasks) {
            if (task.getStatus() == Task.STATUS.QUEUED)
                task.setGroup(newGroup);
        }
        
        return newGroup.getTasks();
    }
    
    public int getPriority() {
        return getGroup(id).priority;
    }
    
    public ImmutableByteArray getId() {
        return id;
    }
}
