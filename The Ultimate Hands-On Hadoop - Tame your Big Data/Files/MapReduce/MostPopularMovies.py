from mrjob.job import MRJob
from mrjob.step import MRStep


class MovieRatings(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sorted_output)
        ]


def mapper_get_ratings(self, _, line):
    # Extract from each input line, the userID and movieID. Yield each movieID mapped to the value 1
    (userID, movieID, rating, timestamp) = line.split("\t")
    yield movieID, 1


def reducer_count_ratings(self, key, values):
    # Aggregate the movies by their number of ratings
    yield str(sum(values)).zfill(5), key


def reducer_sorted_output(self, count, movies):
    # Sort the results by the most popular movie first
    for movie in movies:
        yield movie, count


if __name__ == '__main__':
    MovieRatings.run()
