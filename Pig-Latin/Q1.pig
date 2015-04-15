A = load  'movies_single.dat' using PigStorage(':') as (MOVIEID:int, TITLE:chararray, GENRES:chararray);
B = FILTER A BY GENRES MATCHES '.*Comedy.*' and GENRES MATCHES '.*Drama.*';

C = load  'ratings_single.dat' using PigStorage(':') as (USERID:int, MOVIEID:int, RATING:double, TIMESTAMP:chararray);

D = JOIN B BY MOVIEID, C BY MOVIEID;
/* D: {B::MOVIEID: int,B::TITLE: chararray,B::GENRES: chararray,C::USERID: int,C::MOVIEID: int,C::RATING: double,C::TIMESTAMP: chararray} */

E = FOREACH D GENERATE C::USERID, C::RATING, B::MOVIEID;
/* E: {C::USERID: int,C::RATING: double,B::MOVIEID: int} */

F = group E by MOVIEID;
/* F: {group: int,E: {(C::USERID: int,C::RATING: double,B::MOVIEID: int)}} */

G = foreach F generate group, AVG(E.RATING) as avgRating, E.USERID;
/* G: {group: int,avgRating: double,{(C::USERID: int)}} */

H = group G ALL;
/* H: {group: chararray,G: {(group: int,avgRating: double,{(C::USERID: int)})}} */

I = foreach H generate MIN(G.avgRating);
J = FILTER G BY avgRating == I.$0;
/* J: {group: int,avgRating: double,{(C::USERID: int)}} */

K = foreach J GENERATE group, avgRating, flatten($2);
/* K: {group: int,avgRating: double,null::C::USERID: int} */

L = load 'users_single.dat' using PigStorage(':') as (USERID:int, GENDER:chararray, AGE:int, OCCUPATION:chararray, ZIPCODE:chararray);
M = FILTER L BY GENDER MATCHES '.*M.*' and AGE>20 and AGE<40 and ZIPCODE matches '1.*';

N = Join K by USERID, M by USERID;
/* N: {K::group: int,K::avgRating: double,K::null::C::USERID: int,M::USERID: int,M::GENDER: chararray,M::AGE: int,M::OCCUPATION: chararray,M::ZIPCODE: chararray} */

O = FOREACH N GENERATE M::USERID;
STORE O INTO '/user/root/pig/';
/*Dump O;*/
