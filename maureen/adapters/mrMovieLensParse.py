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
This job parses Movie Lens ratings data: ratings.dat into user vectors
'''

from mrjob.job import MRJob

class MRMovieLensParse(MRJob):
        
    #create user vectors from raw text data in users.dat
    #input: 1181::2094::3::974855786  --> userId, movieID, rating, timeStamp
    #output: userId, [movieId, rating]
    def mapPrepData(self, _, inLine):
        rawArr = inLine.strip().split('::')
        inArr = map(int, rawArr)
        yield inArr[0], [inArr[1], inArr[2]] 
    
    #assemble user vectors
    #input: userId, [movieId, rating]
    #output: userId, [vectorMovies, vectorRatings]
    def reducePrepData(self, userId, values):
        moiveIdArr = []; ratingValArr = []
        for value in values:   
            moiveIdArr.append(value[0])
            ratingValArr.append(value[1])
        yield userId, [moiveIdArr, ratingValArr]
    
    def steps(self):
        return [self.mr(self.mapPrepData, self.reducePrepData)]

if __name__ == '__main__':
    MRMovieLensParse.run() 