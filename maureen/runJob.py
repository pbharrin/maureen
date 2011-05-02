# Copyright 2011 Peter Harrington 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''
This is a simple wrapper that runs mrjob MapReduce jobs, the inputs are:
MRJobClass - the class of the job to be run
argsArr - an array of strings to be used when creating the MRJob.
@author: Peter Harrington  if you have any questions: peter.b.harrington@gmail.com
'''
def runJob(MRJobClass, argsArr):
    #if not local: argsArr.extend(['-r', 'emr'])    #TO DO: add hooks for automatically 
                                                    #appending -r emr 
    mrJob = MRJobClass(args=argsArr)
    runner = mrJob.make_runner()
    runner.run()
    
def runParallelJob(MRJobClass, argsArr):            #TO DO: add threading to allow jobs to run in 
                                                    #parallel 
    #launch a new thread
    #call runJob(MRJobClass, argsArr) on the new thread