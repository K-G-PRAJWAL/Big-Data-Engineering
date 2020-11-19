from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingsBreakdown(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]


def mapper_get_ratings(self, _, line):
    # Extract from each input line, the userID, movieID, rating and timestamp. Yield each rating mapped to the value 1
    (userID, movieID, rating, timestamp) = line.split("\t")
    yield rating, 1


def reducer_count_ratings(self, key, values):
    # Yield the aggregated rating and the total number of such ratings
    yield key, sum(values)


if __name__ == '__main__':
    RatingsBreakdown.run()
