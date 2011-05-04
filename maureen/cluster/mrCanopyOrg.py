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
Created on Apr 27, 2011
This grand job organizes multiple Map Reduce jobs, this is better than having everything 
in one mrjob file because we can store intermediate values to disk rather than passing them 
around.  
@author: Peter Harrington  if you have any questions: peter.b.harrington@gmail.com
'''
from maureen import runJob #can also do from maureen import *
from maureen.adapters import MovieLensParse
from maureen.cluster import MRCanopyCluster
from maureen.cluster import MRCanopyFinalCluster

loc = 'local'         #could be 'local' or 'emr'
if loc == 'local': 
    cwd = 'C:\\Users\\Peter\\workspace\\mrjobTest\\src\\'
    sep = '\\'
elif loc == 'emr': 
    cwd = 's3://rustbucket/'
    sep = '/'

#create User Vectors from MovieLens DataMRMovieLensParse
runJob(MovieLensParse, ['%srat5000.dat' % cwd, '--output-dir=%suVectors' % cwd], loc)

#generate Canopy Clusters 
runJob(MRCanopyCluster, ['%suVectors%spart-00000' % (cwd, sep), 
                           '--output-dir=%sclusters' % cwd, '--t2=64'], loc)

#cluster all user vectors based on canopies generated in the previous step
myJob, myRunner = runJob(MRCanopyFinalCluster, ['%suVectors%spart-00000' % (cwd, sep),
                                                 '--t1=65'], loc)
for line in myRunner.stream_output():  #print output for demonstration
        print myJob.parse_output_line(line)
     
#       TO DO: cleanup delete temporary directories
#   cleanUp(dir)