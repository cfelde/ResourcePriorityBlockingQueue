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

import java.util.Set;

/**
 * @author cfelde
 */
public interface ResourcePrioritizer {
    /**
     * Used to evaluate a given resource against a given task.
     * 
     * The returned value should be higher the better a match a resource
     * is for a given task. This score is then used relative to other scores,
     * as defined by the active ResourceAllocator, when picking which resources
     * to allocate a task to.
     * 
     * If for any reason a given resource should not be allocated a task,
     * null should be returned.
     * 
     * @param task Task to evaluate
     * @param resource Resource to evaluate task against
     * @param allResources A set of all currently connected resources
     * @return Task and resource score (higher represents a better fit), or null to disqualify resource
     */
    public Integer evaluate(Task task, Resource resource, Set<Resource> allResources);
}
