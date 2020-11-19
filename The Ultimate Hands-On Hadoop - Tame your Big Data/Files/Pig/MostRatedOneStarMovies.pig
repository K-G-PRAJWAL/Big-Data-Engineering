ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID: int, movieID: int, rating: int, ratingTime: int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|') AS (movieID: int, movieTitle: chararray, releaseDate: chararray, videoRelease: chararray, imdblink: chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle;

groupedRatings = GROUP ratings by movieID;

averageRatings = FOREACH ratingsByMovie GENERATE GROUP AS movieID, AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS numRatings;

badMovies = FILTER averageRatings BY avgRating < 2.0;

namesBadMovies = JOIN badMovies BY movieID, nameLookup BY movieID;

finalResults = FOREACH namesBadMovies GENERATE nameLookup:: movieTitle AS movieName, badMovies: : avgRating AS avgRating, badMovies: : numRatings AS numRatings;

finalResultsSorted = ORDER finalResults BY numRatings DESC;

DUMP finalResultsSorted;
