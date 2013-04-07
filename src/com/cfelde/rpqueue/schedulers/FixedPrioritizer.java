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
package com.cfelde.rpqueue.schedulers;

import com.cfelde.rpqueue.Resource;
import com.cfelde.rpqueue.ResourcePrioritizer;
import com.cfelde.rpqueue.Task;
import java.util.Set;

/**
 * @author cfelde
 */
public class FixedPrioritizer implements ResourcePrioritizer {
    @Override
    public Integer evaluate(Task task, Resource resource, Set<Resource> allResources) {
        return Integer.MAX_VALUE;
    }
}
