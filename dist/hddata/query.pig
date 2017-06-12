movies = LOAD 'hdfs://master:54310/movies' USING
   PigStorage(',') as (movieId:long,title:chararray);

Dump movies;

ratings = LOAD 'hdfs://master:54310/ratings' USING
PigStorage(',') as (userId:long,movieId:long,rating:float,timestamp:long);


filtered_ratings = FILTER ratings BY (timestamp>=946684800);

joined = JOIN movies BY movieId, ratings BY movieId;

Dump joined;