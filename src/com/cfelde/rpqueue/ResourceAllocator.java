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
import java.util.Set;
import java.util.SortedMap;

/**
 * @author cfelde
 */
public interface ResourceAllocator {
    /**
     * Method should return a set of resources at which the given task should
     * be queued.
     * 
     * A task may be queued at several resources, with the first
     * resources polling the task being the resource at which the task is
     * allocated.
     * 
     * If method returns an empty set (null not allowed), the task will not be
     * queued on any resources. Task will stay unqueued till either a new
     * resource joins the pool or something changes related to the task
     * (priority or group), and this then results in the task being queued.
     * 
     * The given map of resources are sorted in descending order as prioritized
     * by the active ResourcePrioritizer. The returned set of resources must be
     * contained within this map.
     * 
     * @param task Task to assign
     * @param resources Sorted map of resources.
     * @param allResources A set of all currently connected resources
     * @return Set of resources to which task will be queued
     */
    public Set<Resource> assign(Task task, SortedMap<SortedInteger, Resource> resources, Set<Resource> allResources);
}
