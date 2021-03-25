# Report

* Author : Bastien Wermeille
* Scyper : 308542

# Specificities

Here is some explanations of the algorithms implemented during this small project.

## Volcano
### Join

For the join, my first implementation was using 2 LazyList building over the 2 inputs (left and right) allowing in case where we only want to retrieve a single entry to be very efficient. This method was however not as efficient when we need to retrieve all entries so my second implementation loads all the entry from the right and then map those into a map entry to prevent a n*m operations. My implementation is a hash join.

### Aggregate

For the aggregate, we first load all the entries, build the aggregations of the data. The last step is finally to reduce the data before display.

### Sort

An `Ordering` object is created from the parameters and then all data are retrieve and sorted using this ordering.

### Scan

The scan is ensure to load data only once and move forwwards one step at a time in the data, as they are ordered we reconstruct the tuples one line at a time.

### RLEVolcano

### Decode
A simple implementation loading the next line and then keeping it in memory while decrementing an index over the number of time to produce this value.

### Reconstruct

Just iterating over the values to reconstruct if they id match, using some tailrecursive functions.

### RLEAggregate

Using hash map algorithm to aggregate data and then reducing it on the flight to optimise when not all data is needed.

### RLEJoin

Using an hashmap algorithm, hasing th right table and iterating on the flight on the left one.

## RLEVolcano rules

The rules are pretty straight forward, we just push decode and reconstruct as high in the tree as possible.

## Column at a time

The algorithms are the same as used previously except that we need to transpose the data into the tuple form before using it and once done we do another final transpose.

Only `join` and `aggregate` and `sort` remove some tuples ! The other operators simple update the selection column.

## Join

The join has a small optimisation regarding the previous implementation which reside in the fact that we choose the smallest table to create the hashmap as we know the size of each table.

## Operator at a time

Same as column at a time
