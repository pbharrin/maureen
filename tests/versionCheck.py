'''
This file is a simple mrjob map step that simply emits the 
version numbers for different modules.  

We can use this to make sure that bootstrap commands work.

Usage:
>python versionCheck.py -r emr inFile.txt > outFile.txt
'''
from mrjob.job import MRJob
import numpy
import scipy
import platform

class VersionCheck(MRJob):
    
    def map(self, _, lineStr):
        if lineStr == 'numpy':
            yield 'numpy', numpy.__version__
        elif lineStr == 'scipy':
            yield 'scipy', scipy.__version__
        else:
            yield 'python', platform.python_version()
        #the platform package can produce a lot of valuble information
        
    
    def steps(self):
        return [self.mr(self.map), ]

if __name__ == '__main__':
    VersionCheck.run() 