Implement several map reduce design patterns to derive some statistics from IMDB movie data using Hadoop framework. 
Coursework CS 6301


Dataset: License included
ratings.dat UserID::MovieID::Rating::Timestamp
users.dat UserID::Gender::Age::Occupation::Zip-code
movies.dat MovieID::Title::Genres


MAP REDUCE

Question1:
list all male user id whose age is less or equal to 7 .
Using the users.dat file, list all the male userid filtering by age. This demonstrates the
use of mapreduce to filter data.

Question2:
Find the count of female and males users in each age group

Question3:
List all movie title where genre is “fantasy”

********************************************************************************************

Map-Reduce Joins

Q1: Find top 5 average movies rated by female users and print out the titles and
the average rating given to the movie by female users.

Q2.Given the id of a movie, find all userids,gender and age of users who rated the
movie 4 or greater.

*********************************************************************************************
