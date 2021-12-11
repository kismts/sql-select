# sql-select

A js library for performing SQL like queries on arrays of objects.

## Quick start

```sh
npm install sql-select
```

```javascript
import { SELECT, FROM, SUM, AVG, WHERE, GROUP_BY, ORDER_BY, DESC, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input = [
    { name: 'John', salary: 20 },
    { name: 'James', salary: 30 },
    { name: 'Joe', salary: 50 },
    { name: 'John', salary: 100 },
    { name: 'James', salary: 130 },
    { name: 'Joe', salary: 150 },
    { name: 'John', salary: 10 }
]

// returns the selected columns in an array of objects
const query = () =>
SELECT('name',
    SUM('salary', AS('sum')),
    AVG('salary', AS('avg')),
FROM(input),
WHERE(row => row.salary > 15),
GROUP_BY('name'),
ORDER_BY('sum', DESC))

// drawTable turns the result into tabular format
console.log(drawTable(query()))
/*
+-------+-----+-----+
| name  | sum | avg |
+-------+-----+-----+
| Joe   | 200 | 100 |
| James | 160 |  80 |
| John  | 120 |  60 |
+-------+-----+-----+
3 rows selected
*/
```

```javascript
import { SELECT, FROM, SUM, AVG, MAX, OVER, PARTITION_BY, ORDER_BY, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input = [
    { city: 'London', month: 3, sold: 310 },
    { city: 'Paris', month: 2, sold: 200 },
    { city: 'Paris', month: 1, sold: 150 },
    { city: 'London', month: 4, sold: 260 },
    { city: 'Paris', month: 4, sold: 350 },
    { city: 'London', month: 1, sold: 100 },
    { city: 'Paris', month: 3, sold: 400 },
    { city: 'London', month: 2, sold: 250 }
]

// default frame (RANGE_BETWEEN(UNBOUNDED_PRECEDING, CURRENT_ROW)) is used
const win = OVER(PARTITION_BY('city'), ORDER_BY('month'))

// window functions
const query = () =>
SELECT('city', 'month', 'sold',
    SUM('sold', win, AS('running_total')),
    AVG('sold', win, AS('avg_sold')),
    MAX('sold', win, AS('max_sold')),
FROM(input))

console.log(drawTable(query()))
/*
+--------+-------+------+---------------+----------+----------+
| city   | month | sold | running_total | avg_sold | max_sold |
+--------+-------+------+---------------+----------+----------+
| London |     1 |  100 |           100 |      100 |      100 |
| London |     2 |  250 |           350 |      175 |      250 |
| London |     3 |  310 |           660 |      220 |      310 |
| London |     4 |  260 |           920 |      230 |      310 |
| Paris  |     1 |  150 |           150 |      150 |      150 |
| Paris  |     2 |  200 |           350 |      175 |      200 |
| Paris  |     3 |  400 |           750 |      250 |      400 |
| Paris  |     4 |  350 |          1100 |      275 |      400 |
+--------+-------+------+---------------+----------+----------+
8 rows selected
*/
```

## Table of Contents

- [Introduction](#introduction)
- [Select](#select)
- [From and joins](#from-and-joins)
- [Scalar functions](#scalar-functions)
- [Where](#where)
- [Aggregate functions](#aggregate-functions)
- [Groupby](#groupby)
- [Having](#having)
- [Window functions](#window-functions)
    - [Aggregate](#aggregate)
    - [Non aggregate](#non-aggregate)
- [Orderby](#orderby)


## Introduction

### Features:

- direct use of array(s) of objects => no need to build schemas, databases, tables
- functions (instead of strings or object oriented constructs) are used to write queries
- dependency-free, lightweight (less than 15 KB, minified and gzipped)
- can be used even in the browser
- specially written for js objects
- window functions are well supported
- semi-joins are natively supported
- custom js functions can be used as scalar functions
- user defined predicates are used for filtering (where, having)

### Do not use the library for:

- big data => the library is optimized for small and middle sized tables
- complex queries => subqueries, set operations are not supported
- complex data types => only number and string data types are supported

## Select

**A simple query example:**

```javascript
import { SELECT, FROM } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input = [
    { id: 1, name: 'John', age: 22, city: 'London' },
    { id: 2, name: 'Joe', age: 33, city: 'Paris' },
    { id: 3, name: 'James', age: 44, city: 'Berlin' }
]

// returns the selected columns in an array of objects
// all input tables are treated as read-only
// SELECT_TOP, SELECT_DISTINCT, SELECT_DISTINCT_TOP are also supported
const query = () =>
SELECT('id', 'age', 'name',
FROM(input))

// drawTable turns the result into tabular format
console.log(drawTable(query()))
/*
+----+-----+-------+
| id | age | name  |
+----+-----+-------+
|  1 |  22 | John  |
|  2 |  33 | Joe   |
|  3 |  44 | James |
+----+-----+-------+
3 rows selected
*/
```

> ***Note:*** *sql-select-utils library can also be downloaded from npm.*

**With asterisk:**

```javascript
import { SELECT, FROM } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input = [
    { id: 1, name: 'John', age: 22, city: 'London' },
    { id: 2, name: 'Joe', age: 33, city: 'Paris' },
    { id: 3, name: 'James', age: 44, city: 'Berlin' }
]

// returns all columns
const query = () =>
SELECT('*',
FROM(input))

console.log(drawTable(query()))
/*
+----+-------+-----+--------+
| id | name  | age | city   |
+----+-------+-----+--------+
|  1 | John  |  22 | London |
|  2 | Joe   |  33 | Paris  |
|  3 | James |  44 | Berlin |
+----+-------+-----+--------+
3 rows selected
*/
```

**Using heterogenous input table:**

```javascript
import { SELECT, FROM } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input = [
    { id: 1, name: 'John', age: 22 },
    { id: 2, name: 'Joe', age: 33, city: 'Paris' },
    { id: 3, name: 'James', age: undefined },
    { id: 4 }
]

// uses the first row as pattern
// ignores rest columns
// turns missing columns, undefined into null
const query = () =>
SELECT('*',
FROM(input))

console.log(drawTable(query()))
/*
+----+-------+------+
| id | name  |  age |
+----+-------+------+
|  1 | John  |   22 |
|  2 | Joe   |   33 |
|  3 | James | null |
|  4 | null  | null |
+----+-------+------+
4 rows selected
*/
```

## From and joins

**Inner join with ON:**

```javascript
import { SELECT, FROM, JOIN, ON } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input1 = [
    { id: 1, name: 'John', age: 5 },
    { id: 2, name: 'James', age: 15 },
]

const input2 = [
    { num: 1, age: 1 },
    { num: 1, age: 11 },
    { num: 2, age: 22 },
]

// JOIN is the short form for INNER_JOIN
// The first parameter of ON must be a column in the first table
// The second parameter of ON must be a column in the second table
// Any number of joins can be used
// The default names of the tables are t1, t2, t3 and so on
// Equi-join is used in ON
// Columns present in more tables have qualified names
// Columns present in only one table have unqualified names
const query = () =>
SELECT('*',
FROM(input1, JOIN, input2, ON('id', 'num')))

console.log(drawTable(query()))
/*
+----+-------+--------+-----+--------+
| id | name  | t1.age | num | t2.age |
+----+-------+--------+-----+--------+
|  1 | John  |      5 |   1 |      1 |
|  1 | John  |      5 |   1 |     11 |
|  2 | James |     15 |   2 |     22 |
+----+-------+--------+-----+--------+
3 rows selected
*/
```

**Inner join with USING**:

```javascript
import { SELECT, FROM, INNER_JOIN, USING } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input1 = [
    { id: 1, name: 'John', age: 5 },
    { id: 2, name: 'James', age: 15 },
]

const input2 = [
    { id: 1, age: 1 },
    { id: 1, age: 11 },
    { id: 2, age: 22 },
]

// USING can be used when join-columns are present in both tables
// custom table name can also be used
// Equi-join is used in USING
// INNER_JOIN is the explicite form of JOIN
const query = () =>
SELECT('*',
FROM(input1, 'first', INNER_JOIN, input2, 'second', USING('id')))

console.log(drawTable(query()))
/*
+----+-------+-----------+------------+
| id | name  | first.age | second.age |
+----+-------+-----------+------------+
|  1 | John  |         5 |          1 |
|  1 | John  |         5 |         11 |
|  2 | James |        15 |         22 |
+----+-------+-----------+------------+
3 rows selected
*/
```

**Three input tables with ON:**

```javascript
import { SELECT, FROM, JOIN, ON } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input1 = [
    { id: 1, name: 'John' },
    { id: 2, name: 'James' },
]

const input2 = [
    { id: 1, age: 1 },
    { id: 1, age: 11 },
    { id: 2, age: 22 },
]

// using id in subsequent ON-s => qualified name must be used
const query = () =>
SELECT('*',
FROM(input1, JOIN, input2, ON('id', 'id'),
             JOIN, input2, ON('t1.id', 'id')))

console.log(drawTable(query()))
/*
+-------+-------+-------+--------+-------+--------+
| t1.id | name  | t2.id | t2.age | t3.id | t3.age |
+-------+-------+-------+--------+-------+--------+
|     1 | John  |     1 |      1 |     1 |      1 |
|     1 | John  |     1 |      1 |     1 |     11 |
|     1 | John  |     1 |     11 |     1 |      1 |
|     1 | John  |     1 |     11 |     1 |     11 |
|     2 | James |     2 |     22 |     2 |     22 |
+-------+-------+-------+--------+-------+--------+
5 rows selected
*/
```

**Three input tables with USING:**

```javascript
import { SELECT, FROM, JOIN, USING } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input1 = [
    { id: 1, name: 'John' },
    { id: 2, name: 'James' },
]

const input2 = [
    { id: 1, age: 1 },
    { id: 1, age: 11 },
    { id: 2, age: 22 },
]

// in all USING-s unqualified column names are used
const query = () =>
SELECT('*',
FROM(input1, JOIN, input2, USING('id'),
             JOIN, input2, USING('id')))

console.log(drawTable(query()))
/*
+----+-------+--------+--------+
| id | name  | t2.age | t3.age |
+----+-------+--------+--------+
|  1 | John  |      1 |      1 |
|  1 | John  |      1 |     11 |
|  1 | John  |     11 |      1 |
|  1 | John  |     11 |     11 |
|  2 | James |     22 |     22 |
+----+-------+--------+--------+
5 rows selected
*/
```

**Outer join:**

```javascript
import { SELECT, FROM, LEFT_JOIN, ON } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input1 = [
    { id: 1, name: 'A' },
    { id: 2, name: 'B' },
    { id: 3, name: 'C' },
    { id: null, name: 'D' },
    { id: 3, name: 'E' },
    { id: 4, name: 'F' },
    { id: null, name: 'G' }
]

const input2 = [
    { id: 3, age: 1 },
    { id: 4, age: 11 },
    { id: 4, age: 22 },
    { id: null, age: 33 },
    { id: 5, age: 44 },
    { id: 6, age: 55 },
    { id: null, age: 66 }
]

// RIGHT_JOIN and FULL_JOIN are also supported
const query = () =>
SELECT('*',
FROM(input1, LEFT_JOIN, input2, ON('id', 'id')))

console.log(drawTable(query()))
/*
+-------+------+-------+------+
| t1.id | name | t2.id |  age |
+-------+------+-------+------+
|     1 | A    |  null | null |
|     2 | B    |  null | null |
|     3 | C    |     3 |    1 |
|  null | D    |  null | null |
|     3 | E    |     3 |    1 |
|     4 | F    |     4 |   11 |
|     4 | F    |     4 |   22 |
|  null | G    |  null | null |
+-------+------+-------+------+
8 rows selected
*/
```

**Semi join:**

```javascript
import { SELECT, FROM, LEFT_SEMI_JOIN, ON } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input1 = [
    { id: 1, name: 'A' },
    { id: 2, name: 'B' },
    { id: 3, name: 'C' },
    { id: null, name: 'D' },
    { id: 3, name: 'E' },
    { id: 4, name: 'F' },
    { id: null, name: 'G' }
]

const input2 = [
    { id: 3, age: 1 },
    { id: 4, age: 11 },
    { id: 4, age: 22 },
    { id: null, age: 33 },
    { id: 5, age: 44 },
    { id: 6, age: 55 },
    { id: null, age: 66 }
]

// RIGHT_SEMI_JOIN, LEFT_ANTI_JOIN and RIGHT_ANTI_JOIN are also supported
const query = () =>
SELECT('*',
FROM(input1, LEFT_SEMI_JOIN, input2, ON('id', 'id')))

console.log(drawTable(query()))
/*
+----+------+
| id | name |
+----+------+
|  3 | C    |
|  3 | E    |
|  4 | F    |
+----+------+
3 rows selected
*/
```

**Cross join:**

```javascript
import { SELECT, FROM, CROSS_JOIN, ON } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input1 = [
    { id: 1, name: 'A' },
    { id: 2, name: 'B' }
]

const input2 = [
    { id: 3, age: 1 },
    { id: 4, age: 11 }
]

// with crossjoin ON/USING can not be used
// even CROSS_JOIN can be left out
const query = () =>
SELECT('*',
FROM(input1, CROSS_JOIN, input2))

console.log(drawTable(query()))
/*
+-------+------+-------+-----+
| t1.id | name | t2.id | age |
+-------+------+-------+-----+
|     1 | A    |     3 |   1 |
|     1 | A    |     4 |  11 |
|     2 | B    |     3 |   1 |
|     2 | B    |     4 |  11 |
+-------+------+-------+-----+
4 rows selected
*/
```

## Scalar functions

**Simple and nested scalar functions:**

```javascript
import { SELECT, FROM, ADD, SUB, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input = [
    { id: 1, name: 'John', size: 10, type: 8 },
    { id: 2, name: 'Joe', size: 12, type: 6 },
    { id: 3, name: 'James', size: 8, type:4 }
]

// last parameter of every function must be AS (column alias)
// AS can not be used in nested functions
// column aliases can be used in later scalar functions
// strings with colons are treated as values (not column names)
const query = () =>
SELECT('id', 'name', 'size', 'type',
    ADD('size', 'type', AS('add1')),
    ADD(ADD('size', 'type'), 10, SUB('size', 'type'), AS('add2')),
    ADD('add1', 'add2', AS('add3')),
    ADD(':Mr. :', 'name', AS('full_name')),
FROM(input))

console.log(drawTable(query()))
/*
+----+-------+------+------+------+------+------+-----------+
| id | name  | size | type | add1 | add2 | add3 | full_name |
+----+-------+------+------+------+------+------+-----------+
|  1 | John  |   10 |    8 |   18 |   30 |   48 | Mr. John  |
|  2 | Joe   |   12 |    6 |   18 |   34 |   52 | Mr. Joe   |
|  3 | James |    8 |    4 |   12 |   26 |   38 | Mr. James |
+----+-------+------+------+------+------+------+-----------+
3 rows selected
*/
```

**Custom scalar function:**

```javascript
import { SELECT, FROM, AS, createFn } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input = [
    { id: 1, name: 'John' },
    { id: 2, name: 'Joe' },
    { id: 3, name: 'James' }
]

// creating custom scalar function is extremely easy:
// just call createFn with your function and use the returned function
const toUpper = str => str.toUpperCase()
const TO_UPPER = createFn(toUpper)

// using custom scalar function
const query = () =>
SELECT('id', 'name',
    TO_UPPER('name', AS('uppercased_name')),
FROM(input))

console.log(drawTable(query()))
/*
+----+-------+-----------------+
| id | name  | uppercased_name |
+----+-------+-----------------+
|  1 | John  | JOHN            |
|  2 | Joe   | JOE             |
|  3 | James | JAMES           |
+----+-------+-----------------+
3 rows selected
*/
```

## Where

**An example using WHERE:**

```javascript
import { SELECT, FROM, WHERE } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input = [
    { id: 1, name: 'John', size: 10 },
    { id: 2, name: 'Joe', size: 12 },
    { id: 3, name: 'James', size: 8 }
]

// row['size'], row["size"], row[`size`] forms are also acceptable
const query = () =>
SELECT('id', 'name', 'size',
FROM(input),
WHERE(row => row.size > 9))

console.log(drawTable(query()))
/*
+----+------+------+
| id | name | size |
+----+------+------+
|  1 | John |   10 |
|  2 | Joe  |   12 |
+----+------+------+
2 rows selected
*/
```

> ***Note:*** *To get column value: the **above forms** can be used in the **supplied predicate**.*

**Using scalar alias in WHERE:**

```javascript
import { SELECT, FROM, WHERE, ADD, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input = [
    { id: 1, name: 'John', size: 10, type: 10 },
    { id: 2, name: 'Joe', size: 12, type: 6 },
    { id: 3, name: 'James', size: 8, type:4 }
]

// result of scalar functions can also be used in where
const query = () =>
SELECT('id', 'name', 'size', 'type',
    ADD('size', 'type', AS('add')),
FROM(input),
WHERE(row => row.add < 20))

console.log(drawTable(query()))
/*
+----+-------+------+------+-----+
| id | name  | size | type | add |
+----+-------+------+------+-----+
|  2 | Joe   |   12 |    6 |  18 |
|  3 | James |    8 |    4 |  12 |
+----+-------+------+------+-----+
2 rows selected
*/
```


## Aggregate functions

**Normal aggregate functions:**

```javascript
import { SELECT, FROM, COUNT, SUM, AVG, MAX, MIN, ADD, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input = [
    { id: 1, name: 'John', size: 10 },
    { id: 2, name: 'Joe', size: 10 },
    { id: 3, name: 'James', size: null },
    { id: 4, name: 'Peter', size: 4 }
]

// custom aggregate functions are not supported
const query = () =>
SELECT(
    COUNT('*', AS('count_all')),
    COUNT('size', AS('count')),
    SUM('size', AS('sum')),
    AVG('size', AS('avg')),
    MAX('size', AS('max')),
    MIN('size', AS('min')),
    ADD('count_all', 'count', SUM('size'), 'avg', 'max', 'min', AS('total')),
FROM(input))

console.log(drawTable(query()))
/*
+-----------+-------+-----+-----+-----+-----+-------+
| count_all | count | sum | avg | max | min | total |
+-----------+-------+-----+-----+-----+-----+-------+
|         4 |     3 |  24 |   8 |  10 |   4 |    53 |
+-----------+-------+-----+-----+-----+-----+-------+
1 row selected
*/
```

**Special aggregate functions:**

```javascript
import { SELECT, FROM, COUNT_DISTINCT, SUM_DISTINCT, AVG_DISTINCT, PERCENTILE_CONT,
    PERCENTILE_DISC, MEDIAN, WITHIN_GROUP, ORDER_BY, ADD, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input = [
    { id: 1, name: 'John', size: 10 },
    { id: 2, name: 'Joe', size: 10 },
    { id: 3, name: 'James', size: null },
    { id: 4, name: 'Peter', size: 4 }
]

const query = () =>
SELECT(
    COUNT_DISTINCT('size', AS('count_dist')),
    SUM_DISTINCT('size', AS('sum_dist')),
    AVG_DISTINCT('size', AS('avg_dist')),
    PERCENTILE_CONT(0.5, WITHIN_GROUP(ORDER_BY('size')), AS('perc_cont')),
    PERCENTILE_DISC(0.5, WITHIN_GROUP(ORDER_BY('size')), AS('perc_disc')),
    MEDIAN('size', AS('median')),
    ADD('count_dist', 'sum_dist', AVG_DISTINCT('size'), 'perc_cont', 'perc_disc', 'median', AS('total')),
FROM(input))

console.log(drawTable(query()))
/*
+------------+----------+----------+-----------+-----------+--------+-------+
| count_dist | sum_dist | avg_dist | perc_cont | perc_disc | median | total |
+------------+----------+----------+-----------+-----------+--------+-------+
|          2 |       14 |        7 |        10 |        10 |     10 |    53 |
+------------+----------+----------+-----------+-----------+--------+-------+
1 row selected
*/
```

**Scalar and aggregate functions:**

```javascript
import { SELECT, FROM, SUM, MAX, MIN, ADD, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'

const input = [
    { id: 1, name: 'John', size: 10 },
    { id: 2, name: 'Joe', size: 10 },
    { id: 3, name: 'James', size: null },
    { id: 4, name: 'Peter', size: 4 }
]

// scalar functions can be nested in aggregate functions
const query = () =>
SELECT(
    SUM(ADD('size', 'id'), AS('sum')),
    MAX('size', AS('max')),
    MIN('size', AS('min')),
    ADD('sum', SUM(ADD('size', 'id')), 'max', 'min', AS('total')),
FROM(input))

console.log(drawTable(query()))
/*
+-----+-----+-----+-------+
| sum | max | min | total |
+-----+-----+-----+-------+
|  31 |  10 |   4 |    76 |
+-----+-----+-----+-------+
1 row selected
*/
```

## Groupby

*Create a data.js file:*

```javascript

// data.js
// from now on every file will import this array as input
export const input = [
    { id: 1, city: 'London', size: 10 },
    { id: 2, city: 'London', size: 10 },
    { id: 3, city: 'London', size: null },
    { id: 4, city: 'London', size: 4 },

    { id: 5, city: 'Paris', size: 12 },
    { id: 6, city: 'Paris', size: 10 },
    { id: 7, city: 'Paris', size: 8 },
    { id: 8, city: 'Paris', size: 6 },

    { id: 9, city: 'Berlin', size: 10 },
    { id: 10, city: 'Berlin', size: null },
    { id: 11, city: 'Berlin', size: null },
    { id: 12, city: 'Berlin', size: 4 }
]

```

**Normal aggregate functions:**

```javascript
import { SELECT, FROM, GROUP_BY, COUNT, SUM, AVG, MAX, MIN, ADD, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'
import { input } from './data.js'

const query = () =>
SELECT('city',
    COUNT('*', AS('count_all')),
    COUNT('size', AS('count')),
    SUM('size', AS('sum')),
    AVG('size', AS('avg')),
    MAX('size', AS('max')),
    MIN('size', AS('min')),
    ADD('count_all', 'count', 'sum', AVG('size'), 'max', 'min', AS('total')),
FROM(input),
GROUP_BY('city'))

console.log(drawTable(query()))
/*
+--------+-----------+-------+-----+-----+-----+-----+-------+
| city   | count_all | count | sum | avg | max | min | total |
+--------+-----------+-------+-----+-----+-----+-----+-------+
| Berlin |         4 |     2 |  14 |   7 |  10 |   4 |    41 |
| London |         4 |     3 |  24 |   8 |  10 |   4 |    53 |
| Paris  |         4 |     4 |  36 |   9 |  12 |   6 |    71 |
+--------+-----------+-------+-----+-----+-----+-----+-------+
3 rows selected
*/
```

**Special aggregate functions:**

```javascript
import { SELECT, FROM, GROUP_BY, COUNT_DISTINCT, SUM_DISTINCT, AVG_DISTINCT, PERCENTILE_CONT,
    PERCENTILE_DISC, MEDIAN, WITHIN_GROUP, ORDER_BY, ADD, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'
import { input } from './data.js'

const query = () =>
SELECT('city',
    COUNT_DISTINCT('size', AS('count_dist')),
    SUM_DISTINCT('size', AS('sum_dist')),
    AVG_DISTINCT('size', AS('avg_dist')),
    PERCENTILE_CONT(0.5, WITHIN_GROUP(ORDER_BY('size')), AS('perc_cont')),
    PERCENTILE_DISC(0.5, WITHIN_GROUP(ORDER_BY('size')), AS('perc_disc')),
    MEDIAN('size', AS('median')),
    ADD('count_dist', 'sum_dist', 'avg_dist', 'perc_cont', 'perc_disc', MEDIAN('size'), AS('total')),
FROM(input),
GROUP_BY('city'))

console.log(drawTable(query()))
/*
+--------+------------+----------+----------+-----------+-----------+--------+-------+
| city   | count_dist | sum_dist | avg_dist | perc_cont | perc_disc | median | total |
+--------+------------+----------+----------+-----------+-----------+--------+-------+
| Berlin |          2 |       14 |        7 |         7 |         4 |      7 |    41 |
| London |          2 |       14 |        7 |        10 |        10 |     10 |    53 |
| Paris  |          4 |       36 |        9 |         9 |         8 |      9 |    75 |
+--------+------------+----------+----------+-----------+-----------+--------+-------+
3 rows selected
*/
```

**Scalar and aggregate functions:**

```javascript
import { SELECT, FROM, GROUP_BY, SUM, MAX, MIN, ADD, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'
import { input } from './data.js'

// scalar functions can be nested in aggregate functions
const query = () =>
SELECT('city',
    SUM(ADD('size', 'id'), AS('sum')),
    MAX('size', AS('max')),
    MIN('size', AS('min')),
    ADD('sum', 'max', 'min', SUM(ADD('size', 'id')), AS('total')),
FROM(input),
GROUP_BY('city'))

console.log(drawTable(query()))
/*
+--------+-----+-----+-----+-------+
| city   | sum | max | min | total |
+--------+-----+-----+-----+-------+
| Berlin |  35 |  10 |   4 |    84 |
| London |  31 |  10 |   4 |    76 |
| Paris  |  62 |  12 |   6 |   142 |
+--------+-----+-----+-----+-------+
3 rows selected
*/
```


## Having

```javascript
import { SELECT, FROM, GROUP_BY, HAVING, SUM, MAX, MIN, ADD, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'
import { input } from './data.js'

// having can be used to filter aggregate data
// row['sum'], row["sum"], row[`sum`] forms are also acceptable
const query = () =>
SELECT('city',
    SUM(ADD('size', 'id'), AS('sum')),
    MAX('size', AS('max')),
    MIN('size', AS('min')),
    ADD('sum', 'max', 'min', AS('total')),
FROM(input),
GROUP_BY('city'),
HAVING(row => row.sum > 32))

console.log(drawTable(query()))
/*
+--------+-----+-----+-----+-------+
| city   | sum | max | min | total |
+--------+-----+-----+-----+-------+
| Berlin |  35 |  10 |   4 |    49 |
| Paris  |  62 |  12 |   6 |    80 |
+--------+-----+-----+-----+-------+
2 rows selected
*/
```

> ***Note:*** *To get column value: the **above forms** can be used in the **supplied predicate**.*

## Window functions

### Aggregate

**Without PARTITION_BY:**

```javascript
import { SELECT, FROM, ROWS_BETWEEN, UNBOUNDED_PRECEDING, CURRENT_ROW,
    OVER, ORDER_BY, COUNT, SUM, AVG, MAX, MIN, ADD, AS, createFn } from 'sql-select'
import { drawTable } from 'sql-select-utils'
import { input } from './data.js'

// all frames are supported
// RANGE_BETWEEN, GROUPS_BETWEEN are also supported
// short frame forms are also supported => ROWS(UNBOUNDED_PRECEDING)
const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, CURRENT_ROW)
const win = OVER(ORDER_BY('id'), frame)

const round = num => Math.round(num * 100) / 100
const ROUND = createFn(round)

const query = () =>
SELECT('city', 'id', 'size',
    COUNT('*', win, AS('count_all')),
    COUNT('size', win, AS('count')),
    SUM('size', win, AS('sum')),
    ROUND(AVG('size', win), AS('avg')),
    MIN('size', win, AS('min')),
    MAX('size', win, AS('max')),
    ADD('count', 'sum', 'avg', MAX('size', win), 'min', AS('total')),
FROM(input))

console.log(drawTable(query()))
/*
+--------+----+------+-----------+-------+-----+------+-----+-----+--------+
| city   | id | size | count_all | count | sum |  avg | min | max |  total |
+--------+----+------+-----------+-------+-----+------+-----+-----+--------+
| London |  1 |   10 |         1 |     1 |  10 |   10 |  10 |  10 |     41 |
| London |  2 |   10 |         2 |     2 |  20 |   10 |  10 |  10 |     52 |
| London |  3 | null |         3 |     2 |  20 |   10 |  10 |  10 |     52 |
| London |  4 |    4 |         4 |     3 |  24 |    8 |   4 |  10 |     49 |
| Paris  |  5 |   12 |         5 |     4 |  36 |    9 |   4 |  12 |     65 |
| Paris  |  6 |   10 |         6 |     5 |  46 |  9.2 |   4 |  12 |   76.2 |
| Paris  |  7 |    8 |         7 |     6 |  54 |    9 |   4 |  12 |     85 |
| Paris  |  8 |    6 |         8 |     7 |  60 | 8.57 |   4 |  12 |  91.57 |
| Berlin |  9 |   10 |         9 |     8 |  70 | 8.75 |   4 |  12 | 102.75 |
| Berlin | 10 | null |        10 |     8 |  70 | 8.75 |   4 |  12 | 102.75 |
| Berlin | 11 | null |        11 |     8 |  70 | 8.75 |   4 |  12 | 102.75 |
| Berlin | 12 |    4 |        12 |     9 |  74 | 8.22 |   4 |  12 | 107.22 |
+--------+----+------+-----------+-------+-----+------+-----+-----+--------+
12 rows selected
*/
```

**With PARTITION_BY:**

```javascript
import { SELECT, FROM, ROWS_BETWEEN, UNBOUNDED_PRECEDING, CURRENT_ROW,
    OVER, ORDER_BY, PARTITION_BY, COUNT, SUM, AVG, MAX, MIN, ADD, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'
import { input } from './data.js'

const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, CURRENT_ROW)
const win = OVER(PARTITION_BY('city'), ORDER_BY('id'), frame)

const query = () =>
SELECT('city', 'id', 'size',
    COUNT('*', win, AS('count_all')),
    COUNT('size', win, AS('count')),
    SUM('size', win, AS('sum')),
    AVG('size', win, AS('avg')),
    MIN('size', win, AS('min')),
    MAX('size', win, AS('max')),
    ADD('count', 'sum', AVG('size', win), 'max', 'min', AS('total')),
FROM(input))

console.log(drawTable(query()))
/*
+--------+----+------+-----------+-------+-----+-----+-----+-----+-------+
| city   | id | size | count_all | count | sum | avg | min | max | total |
+--------+----+------+-----------+-------+-----+-----+-----+-----+-------+
| Berlin |  9 |   10 |         1 |     1 |  10 |  10 |  10 |  10 |    41 |
| Berlin | 10 | null |         2 |     1 |  10 |  10 |  10 |  10 |    41 |
| Berlin | 11 | null |         3 |     1 |  10 |  10 |  10 |  10 |    41 |
| Berlin | 12 |    4 |         4 |     2 |  14 |   7 |   4 |  10 |    37 |
| London |  1 |   10 |         1 |     1 |  10 |  10 |  10 |  10 |    41 |
| London |  2 |   10 |         2 |     2 |  20 |  10 |  10 |  10 |    52 |
| London |  3 | null |         3 |     2 |  20 |  10 |  10 |  10 |    52 |
| London |  4 |    4 |         4 |     3 |  24 |   8 |   4 |  10 |    49 |
| Paris  |  5 |   12 |         1 |     1 |  12 |  12 |  12 |  12 |    49 |
| Paris  |  6 |   10 |         2 |     2 |  22 |  11 |  10 |  12 |    57 |
| Paris  |  7 |    8 |         3 |     3 |  30 |  10 |   8 |  12 |    63 |
| Paris  |  8 |    6 |         4 |     4 |  36 |   9 |   6 |  12 |    67 |
+--------+----+------+-----------+-------+-----+-----+-----+-----+-------+
12 rows selected
*/
```

### Non aggregate

**Ranking functions:**

```javascript
import { SELECT, FROM, ROW_NUMBER, RANK, DENSE_RANK, OVER, ORDER_BY, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'
import { input } from './data.js'

// for simplicity from now on partitionby is not used
const query = () =>
SELECT('city', 'size',
    ROW_NUMBER(OVER(ORDER_BY('size')), AS('row_num')),
    RANK(OVER(ORDER_BY('size')), AS('rank')),
    DENSE_RANK(OVER(ORDER_BY('size')), AS('dense_rank')),
FROM(input))

console.log(drawTable(query()))
/*
+--------+------+---------+------+------------+
| city   | size | row_num | rank | dense_rank |
+--------+------+---------+------+------------+
| London | null |       1 |    1 |          1 |
| Berlin | null |       2 |    1 |          1 |
| Berlin | null |       3 |    1 |          1 |
| London |    4 |       4 |    4 |          2 |
| Berlin |    4 |       5 |    4 |          2 |
| Paris  |    6 |       6 |    6 |          3 |
| Paris  |    8 |       7 |    7 |          4 |
| London |   10 |       8 |    8 |          5 |
| London |   10 |       9 |    8 |          5 |
| Paris  |   10 |      10 |    8 |          5 |
| Berlin |   10 |      11 |    8 |          5 |
| Paris  |   12 |      12 |   12 |          6 |
+--------+------+---------+------+------------+
12 rows selected
*/
```

**Distribution functions:**

```javascript
import { SELECT, FROM, PERCENT_RANK, CUME_DIST, OVER, ORDER_BY, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'
import { input } from './data.js'

const query = () =>
SELECT('city', 'size',
    PERCENT_RANK(OVER(ORDER_BY('size')), AS('perc_rank')),
    CUME_DIST(OVER(ORDER_BY('size')), AS('cume_dist')),
FROM(input))

console.log(drawTable(query()))
/*
+--------+------+---------------------+--------------------+
| city   | size |           perc_rank |          cume_dist |
+--------+------+---------------------+--------------------+
| London | null |                   0 |               0.25 |
| Berlin | null |                   0 |               0.25 |
| Berlin | null |                   0 |               0.25 |
| London |    4 |  0.2727272727272727 | 0.4166666666666667 |
| Berlin |    4 |  0.2727272727272727 | 0.4166666666666667 |
| Paris  |    6 | 0.45454545454545453 |                0.5 |
| Paris  |    8 |  0.5454545454545454 | 0.5833333333333334 |
| London |   10 |  0.6363636363636364 | 0.9166666666666666 |
| London |   10 |  0.6363636363636364 | 0.9166666666666666 |
| Paris  |   10 |  0.6363636363636364 | 0.9166666666666666 |
| Berlin |   10 |  0.6363636363636364 | 0.9166666666666666 |
| Paris  |   12 |                   1 |                  1 |
+--------+------+---------------------+--------------------+
12 rows selected
*/
```

**Analytic functions:**

```javascript
import { SELECT, FROM, LEAD, LAG, NTILE, OVER, ORDER_BY, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'
import { input } from './data.js'

const win = OVER(ORDER_BY('size'))

const query = () =>
SELECT('city', 'size',
    LEAD('size', win, AS('lead')),
    LEAD('size', 2, win, AS('lead_2')),
    LEAD('size', 2, 0, win, AS('lead_2_0')),
    LAG('size', win, AS('lag')),
    LAG('size', 2, win, AS('lag_2')),
    LAG('size', 2, 0, win, AS('lag_2_0')),
    NTILE(3, win, AS('ntile')),
FROM(input))

console.log(drawTable(query()))
/*
+--------+------+------+--------+----------+------+-------+---------+-------+
| city   | size | lead | lead_2 | lead_2_0 |  lag | lag_2 | lag_2_0 | ntile |
+--------+------+------+--------+----------+------+-------+---------+-------+
| London | null | null |   null |     null | null |  null |       0 |     1 |
| Berlin | null | null |      4 |        4 | null |  null |       0 |     1 |
| Berlin | null |    4 |      4 |        4 | null |  null |    null |     1 |
| London |    4 |    4 |      6 |        6 | null |  null |    null |     1 |
| Berlin |    4 |    6 |      8 |        8 |    4 |  null |    null |     2 |
| Paris  |    6 |    8 |     10 |       10 |    4 |     4 |       4 |     2 |
| Paris  |    8 |   10 |     10 |       10 |    6 |     4 |       4 |     2 |
| London |   10 |   10 |     10 |       10 |    8 |     6 |       6 |     2 |
| London |   10 |   10 |     10 |       10 |   10 |     8 |       8 |     3 |
| Paris  |   10 |   10 |     12 |       12 |   10 |    10 |      10 |     3 |
| Berlin |   10 |   12 |   null |        0 |   10 |    10 |      10 |     3 |
| Paris  |   12 | null |   null |        0 |   10 |    10 |      10 |     3 |
+--------+------+------+--------+----------+------+-------+---------+-------+
12 rows selected
*/
```

**Value functions:**

```javascript
import { SELECT, FROM, FIRST_VALUE, LAST_VALUE, NTH_VALUE, OVER, ORDER_BY, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'
import { input } from './data.js'

const win = OVER(ORDER_BY('size'))

const query = () =>
SELECT('city', 'size',
    FIRST_VALUE('size', win, AS('first_value')),
    LAST_VALUE('size', win, AS('last_value')),
    NTH_VALUE('size', 7, win, AS('nth_value')),
FROM(input))

console.log(drawTable(query()))
/*
+--------+------+-------------+------------+-----------+
| city   | size | first_value | last_value | nth_value |
+--------+------+-------------+------------+-----------+
| London | null |        null |         12 |         8 |
| Berlin | null |        null |         12 |         8 |
| Berlin | null |        null |         12 |         8 |
| London |    4 |        null |         12 |         8 |
| Berlin |    4 |        null |         12 |         8 |
| Paris  |    6 |        null |         12 |         8 |
| Paris  |    8 |        null |         12 |         8 |
| London |   10 |        null |         12 |         8 |
| London |   10 |        null |         12 |         8 |
| Paris  |   10 |        null |         12 |         8 |
| Berlin |   10 |        null |         12 |         8 |
| Paris  |   12 |        null |         12 |         8 |
+--------+------+-------------+------------+-----------+
12 rows selected
*/
```

**Statistical functions:**

```javascript
import { SELECT, FROM, WITHIN_GROUP, PERCENTILE_CONT, PERCENTILE_DISC,
    MEDIAN, OVER, ORDER_BY, AS } from 'sql-select'
import { drawTable } from 'sql-select-utils'
import { input } from './data.js'

const query = () =>
SELECT('city', 'size',
    PERCENTILE_CONT(0.5, WITHIN_GROUP(ORDER_BY('size')), OVER(), AS('perc_cont')),
    PERCENTILE_DISC(0.5, WITHIN_GROUP(ORDER_BY('size')), OVER(), AS('perc_disc')),
    MEDIAN('size', OVER(), AS('median')),
FROM(input))

console.log(drawTable(query()))
/*
+--------+------+-----------+-----------+--------+
| city   | size | perc_cont | perc_disc | median |
+--------+------+-----------+-----------+--------+
| London | null |        10 |        10 |     10 |
| Berlin | null |        10 |        10 |     10 |
| Berlin | null |        10 |        10 |     10 |
| London |    4 |        10 |        10 |     10 |
| Berlin |    4 |        10 |        10 |     10 |
| Paris  |    6 |        10 |        10 |     10 |
| Paris  |    8 |        10 |        10 |     10 |
| London |   10 |        10 |        10 |     10 |
| London |   10 |        10 |        10 |     10 |
| Paris  |   10 |        10 |        10 |     10 |
| Berlin |   10 |        10 |        10 |     10 |
| Paris  |   12 |        10 |        10 |     10 |
+--------+------+-----------+-----------+--------+
12 rows selected
*/
```

## Orderby

```javascript
import { SELECT, FROM, ORDER_BY, ASC, DESC } from 'sql-select'
import { drawTable } from 'sql-select-utils'
import { input } from './data.js'

// ASC, DESC => for normal sorting, ASC is the default
// NOCASE_ASC, NOCASE_DESC => for case insensitive sorting
// ASC_LOC, DESC_LOC => sorting with current locale
const query = () =>
SELECT('city', 'size', 'id',
FROM(input),
ORDER_BY('city', ASC, 'size', DESC, 'id'))

console.log(drawTable(query()))
/*
+--------+------+----+
| city   | size | id |
+--------+------+----+
| Berlin |   10 |  9 |
| Berlin |    4 | 12 |
| Berlin | null | 10 |
| Berlin | null | 11 |
| London |   10 |  1 |
| London |   10 |  2 |
| London |    4 |  4 |
| London | null |  3 |
| Paris  |   12 |  5 |
| Paris  |   10 |  6 |
| Paris  |    8 |  7 |
| Paris  |    6 |  8 |
+--------+------+----+
12 rows selected
*/
```

