'''
Created on Apr 22, 2011
MapReduce Implementation of cosine similarity recommendation engine
@author: Peter
'''
from mrjob.job import MRJob
from numpy import inf, ones, zeros, array, argsort

class MRRecoCos(MRJob):
    DEFAULT_INPUT_PROTOCOL = 'json' #'json_value' ignores the key
    
    def configure_options(self):
        super(MRRecoCos, self).configure_options()
        self.add_passthrough_option(
            '--numitems', dest='numitems', default=5, type='int',
            help='N: number of items in the dataset')
        self.add_passthrough_option(
            '--topitems', dest='topitems', default=10, type='int',
            help='return the top N number of items')
    
    #breaks up user, [item] pair into <itemId, userId> pairs 
    #also creates co-occurance pairs 
    #input: userId    array of items 
    #example: 1    [0, 10, 21] 
    #output: <0, ['U', 1]>, <0, ['U', 1]> and <0:10, ['I', 1]>, <0:21, ['I', 1]> 
    def map(self, userId, itemArr):
        for itemId in itemArr:
            yield itemId, ['U', userId] #create item vector
        lenItemArr = len(itemArr)
        for l in range(lenItemArr):     #create co-occurance values
            for j in range(l + 1, lenItemArr):
                pairKey = '%d:%d' % (itemArr[l], itemArr[j])
                yield pairKey, ['I', 1] #I means inner-product
                pairKey = '%d:%d' % (itemArr[j], itemArr[l])
                yield pairKey, ['I', 1] #yield mirror 
                
    #counts the number of co-occurances for binary inner product and
    #counts the number of items in an item-vector for the binary norm
    #input: <0, ['U', 1]> and <0:10, ['I', 1]> (output for mapper)
    #output: <0, ['D0', 2]> denominator for cosine calc
    #output: <0, ['N', 4]> numerator for cosine calc
    def reduce(self, itemId, values):
        lenCount = 0
        for value in values:   #count num users in item vector and
            lenCount += 1
            lastValueType = value[0]
            if lastValueType == 'U': yield itemId, value    #pass user prefs allong to later stage
        if (lastValueType == 'U'): #magnitude (item vector norm)
            for i in range(self.options.numitems):#broadcast to every item-item pair
                if i != itemId: 
                    pairKey = '%d:%d' % (itemId, i)
                    yield pairKey, ['D0', lenCount] #lenCount is norm of first vector in key
                    pairKey = '%d:%d' % (i, itemId)
                    yield pairKey, ['D1', lenCount] #lenCount is norm of second vector in key
        elif (lastValueType == 'I'):
            yield itemId, ['N', lenCount]
            
    #assembles the cosine similarity scores: A*B/||A||/||B|| for a single item-item pair
    #inputs: <itemId, ['N', 1.6]> numerator, <itemId, ['D0', 1.0]> denominator
    #output: <cloumnNum     ,[rowNum, simScore]>
    #example: <0, [1, 0.5]> items 0 and 1 have a sim score of 0.5
    def assembleSimScores(self, pairId, values):
        num = 0.0; d0 = inf; d1 = inf #create default values in case of missing values
        for value in values:
            currVal = value[0]
            if currVal == 'N': num = float(value[1])
            elif currVal == 'D0': d0 = float(value[1])
            elif currVal == 'D1': d1 = float(value[1])
            else: yield pairId, value #pass through other values
        if currVal == 'U': return
        matCoords = pairId.split(':')
        yield int(matCoords[0]), ['V', int(matCoords[1]), num/d0/d1]
        
    #create columns of similarity matrix - full rank (not sparse)
    #input: <cloumnNum     ,[rowNum, simScore]>
    #output <columnNum, ['V', columnVector]> example: <3, ['V', [0.0, 0.25, 0.0, 1.0, 0.3]]>
    def assembleSimVectors(self, columnId, values):
        columnVec = ones(self.options.numitems)
        for value in values:
            if value[0] == 'V': columnVec[value[1]] = value[2]
            else: yield columnId, value
        yield columnId, ['C', list(columnVec)]
        
    #first half of distributed matrix mult by partial products
    #for each item-user pair emit the columnVector with userId as the key
    #these will get sorted by user and summed up to get the user prefrence scores
    #input: <columnNum, ['V', columnVector]> and <itemNum, ['U', userNum]>
    #output: <userId, ['S', columnVector]>
    def matrixMultCreateParts(self, itemId, values):
        userList = []
        for value in values:
            if value[0] == 'C': columnVector = value[1]
            elif value[0] == 'U': userList.append(value[1])
        for userId in userList:
            yield userId, ['S', columnVector]
            
    #accumulate matrix mult partial products for each user
    #sort the scores from highest to lowest, and remove items the user already rated
    #input: <userId, ['S', columnVector]>
    #output: <userId, [list of itemIds in descending order]>
    def accumAndRecommend(self, userId, values):
        userVec = zeros(self.options.numitems)
        itemsUserAlreadySaw = []
        for value in values:
            if value[0] == 'S': userVec += array(value[1])
        sortedVals = argsort(userVec)#sort scores ascending - argsort() gives us the index of the sort
        #yield userId, list(userVec) #these are the scores
        yield userId, list(sortedVals[:-(self.options.topitems+1):-1])
    
    def steps(self):
        return [self.mr(self.map, self.reduce), #calculate components of Cosine sim scores
                self.mr(reducer=self.assembleSimScores), #assemble cosine sim scores
                self.mr(reducer=self.assembleSimVectors),
                self.mr(reducer=self.matrixMultCreateParts),
                self.mr(reducer=self.accumAndRecommend),]

if __name__ == '__main__':
    MRRecoCos.run() 