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
This file is one MapReduce job that creates canopy centers for canopy clustering.
The actual clustering is done in another job. 
'''

from mrjob.job import MRJob
from scipy.sparse import coo_matrix
from numpy import sqrt, zeros, array

class MRCanopyCluster(MRJob):
    DEFAULT_INPUT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(MRCanopyCluster, self).__init__(*args, **kwargs)
        self.canopyList = []
    
    def configure_options(self):
        super(MRCanopyCluster, self).configure_options()
        self.add_passthrough_option(
            '--t2', dest='t2', default=40, type='int',
            help='T2: outer distance for canopy generation')
        self.add_passthrough_option(
            '--numitems', dest='numitems', default=3953, type='int',
            help='N: number of items in the dataset')
        
    #this is a helper function that is used in both mapper and reducer
    def canCluster(self, values):
        inVec = coo_matrix((array(values[1]), (zeros(len(values[0])), array(values[0]))), shape=(1,self.options.numitems))#create sparse vector
        if len(self.canopyList) == 0: self.canopyList.append(inVec)
        else:
            for canCenter in self.canopyList:#for each canopy center in the canopyList
                delta = canCenter - inVec       #calc distance
                dist = sqrt(delta.multiply(delta).sum())
                if dist < self.options.t2: #check if the current vector is within T2
                    return #too close to a canopy - exit
            self.canopyList.append(inVec)
            #if it's not within T2 of any center it becomes it's own center
            
    def mapCanopyGen(self, userId, values):#as data comes in compare to canopies
        if False: yield
        self.canCluster(values)
                
    #when all the data has passed through the mapper the canopy centers are 
    #sent out to one mapper with the common key 'PeterIsAwesome'
    def mapFinCanopyGen(self):
        for canCenter in self.canopyList:
            yield 'Peter', [list(canCenter.col), list(canCenter.data)]
            
    def reduceCanGen(self, _, valueGen):
        for values in valueGen:
            self.canCluster(values)
        for canCenter in self.canopyList:#after clustering everything again output the final clusters
            yield list(canCenter.col), list(canCenter.data)
    
    def steps(self):
        return [self.mr(mapper=self.mapCanopyGen, 
                        mapper_final=self.mapFinCanopyGen, 
                        reducer=self.reduceCanGen), #calculate canopy centers
                ]

if __name__ == '__main__':
    MRCanopyCluster.run() 