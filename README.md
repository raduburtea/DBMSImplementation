# Database Management System Implementation 

In this project I implement a predefined three way join from scratch in C++.

The query used is the following:
```
select sum(large1.c * large2.c * small.c) from large1, large2, large3 where large1.a = large2.a and large2.a
 = small.a and x*large1.b + y*large2.b + z*small.b > threshold;
 ```
 
 The implementation uses a hash join for the *large2* and *small* relations and a sort-merge join for the resulting intermediate view and the *large1* table.
 The only table that contains duplicates is *small*, which is also used as the build side in the hash join since it is smaller.
