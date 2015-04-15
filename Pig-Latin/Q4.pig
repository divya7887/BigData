REGISTER FORMAT_GENRE_JAVA_PIG.jar;
Movies = load 'movies_single.dat' using PigStorage(':') as (MovieID:chararray, Title:chararray, Genres:chararray);
result = FOREACH Movies GENERATE Title, FORMAT_GENRE_JAVA_PIG(Genres);
STORE result INTO '/user/root/pig4/';
DUMP result;
