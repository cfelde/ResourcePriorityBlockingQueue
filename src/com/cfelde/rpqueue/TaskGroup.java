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
    
    // Map contains all tasks assigned to a particular group instance
    private final ConcurrentHashMap<ImmutableByteArray, Task> tasks = new ConcurrentHashMap<ImmutableByteArray, Task>();
    
    // The ZERO group is a static group with a fixed zero priority value.
    // This is the default group used by tasks if no other group is assigned.
    public static final ImmutableByteArray ZERO_ID = new ImmutableByteArray("ZERO".getBytes(CommonUtils.getCharset()));
    public static final TaskGroup ZERO = new TaskGroup(ZERO_ID, 0);
    
    // A static map containing all registered groups, including the ZERO group.
    private static final ConcurrentHashMap<ImmutableByteArray, TaskGroup> groups = new ConcurrentHashMap<ImmutableByteArray, TaskGroup>();
    
    static {
        groups.put(ZERO_ID, ZERO);
    }

    /**
     * Get task group instance associated with given id.
     * 
     * @param id Task group ID
     * @return Task group
     */
    public static TaskGroup getGroup(byte[] id) {
        return getGroup(new ImmutableByteArray(id));
    }
    
    /**
     * Get task group instance associated with given id.
     * 
     * @param id Task group ID
     * @return Task group
     */
    public static TaskGroup getGroup(ImmutableByteArray id) {
        return groups.get(id);
    }
    
    /**
     * Create group associated with given ID and default priority of zero.
     * 
     * If a group by given ID already exists the existing instance is returned
     * with no changes to existing group priority.
     * 
     * @param id Group ID
     * @return Group instance
     */
    public static TaskGroup createGroup(byte[] id) {
        return createGroup(id, 0);
    }
    
    /**
     * Create group associated with given ID and default priority of zero.
     * 
     * If a group by given ID already exists the existing instance is returned
     * with no changes to existing group priority.
     * 
     * @param id Group ID
     * @return Group instance
     */
    public static TaskGroup createGroup(ImmutableByteArray id) {
        return createGroup(id, 0);
    }
    
    /**
     * Create group associated with given ID and given priority.
     * 
     * If a group by given ID already exists the existing instance is returned
     * with no changes to existing group priority.
     * 
     * @param id Group ID
     * @param priority Group priority
     * @return Group instance
     */
    public static TaskGroup createGroup(byte[] id, int priority) {
        return createGroup(new ImmutableByteArray(id), priority);
    }
    
    /**
     * Create group associated with given ID and given priority.
     * 
     * If a group by given ID already exists the existing instance is returned
     * with no changes to existing group priority.
     * 
     * @param id Group ID
     * @param priority Group priority
     * @return Group instance
     */
    public static TaskGroup createGroup(ImmutableByteArray id, int priority) {
        groups.putIfAbsent(id, new TaskGroup(id, priority));
        
        return groups.get(id);
    }

    /**
     * Returns an unmodifiable map of all registered groups.
     * 
     * @return Map of all groups, associated by group ID
     */
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

    /**
     * Returns true if given task is part of group.
     * 
     * @param task Task
     * @return True if task is part of group
     */
    public synchronized boolean haveTask(Task task) {
        return getGroup(id).tasks.containsKey(task.getId());
    }

    /**
     * Returns an unmodifiable collection of tasks who belong to group.
     * 
     * @return Collection of tasks on group
     */
    public Collection<Task> getTasks() {
        return Collections.unmodifiableCollection(getGroup(id).tasks.values());
    }

    /**
     * Returns a count for number of tasks on group.
     * 
     * @return Task count
     */
    public int size() {
        return getGroup(id).tasks.size();
    }

    /**
     * Change the priority of this group.
     * 
     * All tasks associated with this group will be invalidated, i.e. set to
     * canceled (see {@link Task.STATUS), and recreated with the new group
     * priority.
     * 
     * @param priority New group priority
     * @return Collection of group tasks
     */
    public synchronized Collection<Task> setPriority(int priority) {
        if (ZERO_ID.equals(id))
            throw new UnsupportedOperationException("Priority can not be changed on ZERO group");
        
        if (getGroup(id).priority == priority)
            return getTasks();
        
        TaskGroup newGroup = new TaskGroup(id, priority);
        TaskGroup oldGroup = groups.get(id);
        
        Collection<Task> oldTasks = oldGroup.getTasks();
        
        // A concurrent modification exception shouldn't be possible here
        // since related methods (including this) should be synchrpnized.
        // We can do a simple reference check since a group is immutable.
        if (oldGroup != groups.put(id, newGroup)) {
            // Clean up by putting old group back on map..
            groups.put(id, oldGroup);
            
            throw new ConcurrentModificationException();
        }
        
        // Move all tasks from old group to new group. This act will also
        // re-queue the task with the new priority settings.
        for (Task task: oldTasks) {
            if (task.getStatus() == Task.STATUS.QUEUED)
                task.setGroup(newGroup);
        }
        
        return newGroup.getTasks();
    }

    /**
     * Return the group priority setting.
     * 
     * @return Group priority
     */
    public int getPriority() {
        return getGroup(id).priority;
    }

    /**
     * Return group ID
     * 
     * @return Group ID
     */
    public ImmutableByteArray getId() {
        return id;
    }
}
