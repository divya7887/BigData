Movies = load 'movies_single.dat' using PigStorage(':') as (MovieID:chararray, Title:chararray, Genres:chararray);
Ratings = load 'ratings_single.dat' using PigStorage(':') as (UserID:chararray, MovieID:chararray, Rating:double, Timestamp:int);
MoviesRatings = cogroup Movies by (MovieID), Ratings by (MovieID);
result = limit MoviesRatings 6;
STORE result INTO '/user/root/pig2/';
dump result;
