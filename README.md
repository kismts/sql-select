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
- dependency-free, lightweight (less than 8KB, minified and gzipped)
- can be used even in the browser
- specially written for js objects
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
