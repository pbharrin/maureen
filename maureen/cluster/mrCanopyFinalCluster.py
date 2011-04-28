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
Created on Apr 26, 2011
This file is a map-only job which does the clustering.  The canopy centers 
are stored in a local file.  
@author: Peter
'''
from mrjob.job import MRJob
from scipy.sparse import coo_matrix
from numpy import sqrt, zeros, array
import json

class MRCanopyFinalCluster(MRJob):
    DEFAULT_INPUT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(MRCanopyFinalCluster, self).__init__(*args, **kwargs)
        self.canopyList = []
    
    def configure_options(self):
        super(MRCanopyFinalCluster, self).configure_options()
        self.add_passthrough_option(
            '--t1', dest='t1', default=50, type='int',
            help='T1: inner distance for canopy generation')
        self.add_passthrough_option(
            '--numitems', dest='numitems', default=3953, type='int',
            help='N: number of items in the dataset')
        
    def loadCanopyList(self): #loads the canopy list once per mapper
        for line in open('C:\Users\Peter\workspace\mrjobTest\src\clusters\part-00000').readlines():
            lineArr = line.strip().split('\t')
            valuesList = json.loads(lineArr[1])
            colList = json.loads(lineArr[0])
            sprsCanopy = coo_matrix((array(valuesList), (zeros(len(colList)), array(colList))), shape=(1,self.options.numitems))
            self.canopyList.append(sprsCanopy)
    
    def mapFinalCluster(self, userId, inLine):
        if len(self.canopyList) == 0: self.loadCanopyList()
        inVec = coo_matrix((array(inLine[1]), (zeros(len(inLine[0])), array(inLine[0]))), shape=(1,self.options.numitems))
        inAcluster = False
        for i in range(len(self.canopyList)):
            delta = self.canopyList[i] - inVec       #calc distance
            dist = sqrt(delta.multiply(delta).sum())
            if dist < self.options.t1:              
                yield userId, [i, dist]              
                inAcluster = True
        if not inAcluster: yield userId, ['None']  #if not in a cluster-> emit None
        
    def steps(self):
        return [self.mr(mapper=self.mapFinalCluster)]

if __name__ == '__main__':
    MRCanopyFinalCluster.run()