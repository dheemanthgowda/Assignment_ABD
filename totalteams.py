
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys  
class MRTotalTeams(MRJob):
    def mapper1(self, _, lines):
        data = lines.split(',')
        teams = data[3].strip()
        yield teams,None
    
    def combiner(self, key, _):
        yield "total teams",key			
    
    def reducer2(self, key,list_of_teams):
        yield key,len(list(list_of_teams))
    
    def steps(self):
        return [ MRStep(mapper=self.mapper1,reducer=self.combiner),MRStep(reducer=self.reducer2)]



if __name__ == '__main__':
    MRTotalTeams.run()





