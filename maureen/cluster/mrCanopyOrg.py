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
from mrMovieLensParse import MRMovieLensParse
from mrCanopyCluster import MRCanopyCluster
from mrCanopyFinalCluster import MRCanopyFinalCluster

def runMRjob(MRJobClass, argsArr):
    mrJob = MRJobClass(args=argsArr)
    runner = mrJob.make_runner()
    runner.run()
    
cwd = 'C:\\Users\\Peter\\workspace\\mrjobTest\\src\\'

#create User Vectors from MovieLens DataMRMovieLensParse
runMRjob(MRMovieLensParse, ['%srat5000.dat' % cwd, '--output-dir=%s\\uVectors' % cwd])

#generate Canopy Clusters 
runMRjob(MRCanopyCluster, ['%s\\uVectors\\part-00000' % cwd, 
                           '--output-dir=%s\\clusters' % cwd, '--t2=15'])

#cluster all user vectors based on canopies generated in the previous step
mrJob = MRCanopyFinalCluster(args=['%s\\uVectors\\part-00000' % cwd, '--t1=45'])
runner = mrJob.make_runner()
runner.run()
for line in runner.stream_output():  #print output for demonstration
        print mrJob.parse_output_line(line)