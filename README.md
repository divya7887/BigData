Implement several map reduce design patterns to derive some statistics from IMDB movie data using Hadoop framework. 


Dataset: License included
ratings.dat UserID::MovieID::Rating::Timestamp
users.dat UserID::Gender::Age::Occupation::Zip-code
movies.dat MovieID::Title::Genres


Map Reduce

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

Pig Latin

Q1:Using Pig Latin script, list the unique userid of male users whose age between 20 - 40 and who has rated the lowest rated Comedy AND Drama movies. (You should consider all movies that has Comedy AND Drama both in its genre list.)
Print only users whose zip starts with 1. Consider average rating to calculate the lowest rated movies. While finding the Comedy and Drama movies, you should count all users not only the male users.

Q2:Using Pig Latin script, Implement co-group command on MovieID for the datasets ratings and movies. Print first 6 rows.

Q3:Repeat Question 2 (implement join) with co-group commands. Print first 6 rows.

Q4:Write a UDF(User Define Function) FORMAT_GENRE in Pig which basically formats the genre in movie in the following:
Before formatting: Children's

After formatting: 1) Children's <NetId>

Before formatting: Animation|Children's

After formatting: 1) Children's & 2) Animation <NetId>

Before formatting: Children's|Adventure|Animation

After formatting: 1) Children's, 2) Adventure & 3) Animation <NetId>

Using Pig Latin script, use the FORMAT_GENRE function on movies dataset and print the movie name with genres


*********************************************************************************************

Hive

Q5:Using Hive script, find top 11 average rated “Comedy" movies with descending order of rating. (Show the create table command, load from local, and the Hive query).


Q6:Using Hive script, List all the movies with its genre where the movie genre is Comedy or Drama and the average movie rating is in between 4.5 - 4.6 (inclusive) and only the male users rate the movie. (Show the create table command, load from local, and the Hive query).
Q7:Dataset:
Datasets:  January.dat, February.dat and March.dat. The datasets are semi-colon (;) separated and each line has the following 3 columns MovieID;Title;Genres

Requirement:
Using Hive script, create one table partitioned by month. (Show the create table one command, load from local three commands, and one Hive query that selects all columns from the table for the virtual column month of March).
Q8:Requirement:
Create three tables that have three columns each (MovieID, MovieName, Genre). Each table will
represent a month. The three months are January, February and March.
Using Hive multi-table insert, insert values from the table you created in Q7 to these three tables (each table should have names of movies e.g. movies_march etc. for the specified month).

Q9:Write a UDF(User Define Function) FORMAT_GENRE in Hive which basically formats the genre in movies:
Before formatting: Children's

After formatting: 1) Children's <NetId> :hive

Before formatting: Animation|Children's

After formatting: 1) Children's & 2) Animation <NetId> :hive

Before formatting: Children's|Adventure|Animation

After formatting: 1) Children's, 2) Adventure & 3) Animation <NetId> :hive


*********************************************************************************************

Cassandra

*********************************************************************************************
Q10: Cassandra CLI
{cs6360:~} /usr/local/apache-cassandra-2.0.5/bin/cassandra-cli --host csac0
Requirements:

Using Cassandra CLI, write commands to do the following:
1- Create a COLUMN FAMILY for this dataset.
2- Insert the following to the column family created in step 1. Use UserID as the key.
i. "13:F:51:1:93334"
ii. "1471:F:31:17:11116"
iii. "1496:F:31:17:94118" with time to live (ttl) clause after 300 seconds

3- Show the following:
i. Get the Gender and Occupation for user with id 13 ?
ii. Retrieve all rows and columns.
iii. Delete column Gender for the user id 1471.
iv. Drop the column family.

4. Use describe keyspace command with your netid and show content.
5. 

Q11: Cassandra CQL3
{cs6360:~} /usr/local/apache-cassandra2.0.5/bin/cqlsh –3 csac0
Requirements:
Using Cassandra CQL3, write commands to do the following:
1- Create a table for this dataset. Use (UserID) as the Primary Key.
2- Load all records in the dataset to this table.
3- Insert record “6041:M:32:6:11120" to the table.
4- Select the tuple which has user id 6020
5- Delete all rows in the table.
6- Drop the table.


Q12: Cassandra Administration
1) Run nodetool command and determine how much unbalanced the cluster is.
