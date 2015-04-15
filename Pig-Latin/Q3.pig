Movies = load 'movies_single.dat' using PigStorage(':') as (MovieID:chararray, Title:chararray, Genres:chararray);
Ratings = load 'ratings_single.dat' using PigStorage(':') as (UserID:chararray, MovieID:chararray, Rating:double, Timestamp:int);
MovieRatings = cogroup Movies by (MovieID), Ratings by (MovieID);
flat = foreach MovieRatings generate flatten(Movies), flatten(Ratings);
result = limit flat 6;
STORE result INTO '/user/root/pig3/';
dump result;
