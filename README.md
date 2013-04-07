ResourcePriorityBlockingQueue is a blocking queue implementation that:

1. Allows you to assign priority on tasks, just as with a PriorityBlockingQueue.

2. Tasks may belong to task groups where each group may have a different
   priority. In that case, tasks are prioritized by group first, then task
   priority second.

3. For a particular instance of a ResourcePriorityBlockingQueue you give it
   implementations of a ResourcePrioritizer and a ResourceAllocator, which
   further defines to which resources a particular task is made available.

4. Focus as been put on making the code highly concurrent with as little
   synchronized code as possible. Lock free code is used where applicable,
   with efficient use of internal data structures. This gives
   ResourcePriorityBlockingQueue a performance characteristic which is
   comparable to that of a pure PriorityBlockingQueue when used in a similar
   fashion.

Further information available here:

http://blog.cfelde.com/2013/04/resource-aware-queue/

Copyright 2013 Christian Felde (cfelde [at] cfelde [dot] com)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
