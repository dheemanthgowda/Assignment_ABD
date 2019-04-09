
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys  
class MRTotalPlayers(MRJob):
    def mapper1(self, _, lines):
        data = lines.split(',')
        players = data[0].strip()
        yield players,None
    
    def combiner(self, key, _):
        yield "total players",key			
    
    def reducer2(self, key,list_of_players):
        yield key,len(list(list_of_players))
    
    def steps(self):
        return [ MRStep(mapper=self.mapper1,reducer=self.combiner),MRStep(reducer=self.reducer2)]



if __name__ == '__main__':
    MRTotalPlayers.run()





