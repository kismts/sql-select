import { SELECT, FROM, JOIN, ON, ORDER_BY, AS, OVER,
    COUNT, SUM, AVG, MIN, MAX, PARTITION_BY, PRECEDING, ROWS_BETWEEN,
    UNBOUNDED_PRECEDING, CURRENT_ROW, FOLLOWING, UNBOUNDED_FOLLOWING,
    ROWS, RANGE_BETWEEN, RANGE, GROUPS_BETWEEN, GROUPS } from '../../dist/select.js'
import { input4, input5, createTable } from './data.js'
import { drawTable } from './utils.js'
const trim = str => str.trim();

import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const sqlite = require('better-sqlite3');
const db = sqlite(':memory:');


createTable(db, input4, 'input4')
createTable(db, input5, 'input5')







describe('window functions -> (rows_between + frames):', () => {
    it(`1`, () => {
        const table = `
+----------+------+------+-------+------+--------------------+------+------+
| city     | name | size | count |  sum |                avg |  min |  max |
+----------+------+------+-------+------+--------------------+------+------+
| Berlin   | A    |    5 |     0 | null |               null | null | null |
| Berlin   | B    |    1 |     1 |    5 |                  5 |    5 |    5 |
| Berlin   | B    |    2 |     2 |    6 |                  3 |    1 |    5 |
| Berlin   | B    |    3 |     3 |    8 | 2.6666666666666665 |    1 |    5 |
| Berlin   | B    |    7 |     4 |   11 |               2.75 |    1 |    5 |
| Berlin   | C    |    1 |     5 |   18 |                3.6 |    1 |    7 |
| Berlin   | C    |    2 |     6 |   19 | 3.1666666666666665 |    1 |    7 |
| Berlin   | C    |    3 |     7 |   21 |                  3 |    1 |    7 |
| Berlin   | C    |    3 |     8 |   24 |                  3 |    1 |    7 |
| Budapest | B    | null |     0 | null |               null | null | null |
| Budapest | B    | null |     0 | null |               null | null | null |
| Budapest | B    |    5 |     0 | null |               null | null | null |
| Budapest | B    |   11 |     1 |    5 |                  5 |    5 |    5 |
| Budapest | C    | null |     2 |   16 |                  8 |    5 |   11 |
| Budapest | C    | null |     2 |   16 |                  8 |    5 |   11 |
| Budapest | C    |    3 |     2 |   16 |                  8 |    5 |   11 |
| London   | A    |    8 |     0 | null |               null | null | null |
| London   | A    |    8 |     1 |    8 |                  8 |    8 |    8 |
| London   | A    |    8 |     2 |   16 |                  8 |    8 |    8 |
| London   | A    |    9 |     3 |   24 |                  8 |    8 |    8 |
| London   | C    | null |     4 |   33 |               8.25 |    8 |    9 |
| London   | C    |    2 |     4 |   33 |               8.25 |    8 |    9 |
| Madrid   | B    |    2 |     0 | null |               null | null | null |
| Madrid   | B    |    3 |     1 |    2 |                  2 |    2 |    2 |
| Madrid   | B    |   22 |     2 |    5 |                2.5 |    2 |    3 |
| Paris    | A    | null |     0 | null |               null | null | null |
| Paris    | A    |    4 |     0 | null |               null | null | null |
| Paris    | A    |    4 |     1 |    4 |                  4 |    4 |    4 |
| Paris    | A    |    4 |     2 |    8 |                  4 |    4 |    4 |
| Paris    | A    |    7 |     3 |   12 |                  4 |    4 |    4 |
| Rome     | A    | null |     0 | null |               null | null | null |
| Rome     | A    |    4 |     0 | null |               null | null | null |
| Rome     | B    | null |     1 |    4 |                  4 |    4 |    4 |
| Rome     | C    |    2 |     1 |    4 |                  4 |    4 |    4 |
| Rome     | C    |   10 |     2 |    6 |                  3 |    2 |    4 |
| Rome     | C    |   11 |     3 |   16 |  5.333333333333333 |    2 |   10 |
+----------+------+------+-------+------+--------------------+------+------+
36 rows selected`;

        const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, 1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`2`, () => {
        const table = `
+----------+------+------+-------+------+--------------------+------+------+
| city     | name | size | count |  sum |                avg |  min |  max |
+----------+------+------+-------+------+--------------------+------+------+
| Berlin   | A    |    5 |     1 |    5 |                  5 |    5 |    5 |
| Berlin   | B    |    1 |     2 |    6 |                  3 |    1 |    5 |
| Berlin   | B    |    2 |     3 |    8 | 2.6666666666666665 |    1 |    5 |
| Berlin   | B    |    3 |     4 |   11 |               2.75 |    1 |    5 |
| Berlin   | B    |    7 |     5 |   18 |                3.6 |    1 |    7 |
| Berlin   | C    |    1 |     6 |   19 | 3.1666666666666665 |    1 |    7 |
| Berlin   | C    |    2 |     7 |   21 |                  3 |    1 |    7 |
| Berlin   | C    |    3 |     8 |   24 |                  3 |    1 |    7 |
| Berlin   | C    |    3 |     9 |   27 |                  3 |    1 |    7 |
| Budapest | B    | null |     0 | null |               null | null | null |
| Budapest | B    | null |     0 | null |               null | null | null |
| Budapest | B    |    5 |     1 |    5 |                  5 |    5 |    5 |
| Budapest | B    |   11 |     2 |   16 |                  8 |    5 |   11 |
| Budapest | C    | null |     2 |   16 |                  8 |    5 |   11 |
| Budapest | C    | null |     2 |   16 |                  8 |    5 |   11 |
| Budapest | C    |    3 |     3 |   19 |  6.333333333333333 |    3 |   11 |
| London   | A    |    8 |     1 |    8 |                  8 |    8 |    8 |
| London   | A    |    8 |     2 |   16 |                  8 |    8 |    8 |
| London   | A    |    8 |     3 |   24 |                  8 |    8 |    8 |
| London   | A    |    9 |     4 |   33 |               8.25 |    8 |    9 |
| London   | C    | null |     4 |   33 |               8.25 |    8 |    9 |
| London   | C    |    2 |     5 |   35 |                  7 |    2 |    9 |
| Madrid   | B    |    2 |     1 |    2 |                  2 |    2 |    2 |
| Madrid   | B    |    3 |     2 |    5 |                2.5 |    2 |    3 |
| Madrid   | B    |   22 |     3 |   27 |                  9 |    2 |   22 |
| Paris    | A    | null |     0 | null |               null | null | null |
| Paris    | A    |    4 |     1 |    4 |                  4 |    4 |    4 |
| Paris    | A    |    4 |     2 |    8 |                  4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                  4 |    4 |    4 |
| Paris    | A    |    7 |     4 |   19 |               4.75 |    4 |    7 |
| Rome     | A    | null |     0 | null |               null | null | null |
| Rome     | A    |    4 |     1 |    4 |                  4 |    4 |    4 |
| Rome     | B    | null |     1 |    4 |                  4 |    4 |    4 |
| Rome     | C    |    2 |     2 |    6 |                  3 |    2 |    4 |
| Rome     | C    |   10 |     3 |   16 |  5.333333333333333 |    2 |   10 |
| Rome     | C    |   11 |     4 |   27 |               6.75 |    2 |   11 |
+----------+------+------+-------+------+--------------------+------+------+
36 rows selected`;

        const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        const frame2 = ROWS(UNBOUNDED_PRECEDING)
        const win2 = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame2)
        
        const res2 =
        SELECT('city', 'name', 'size',
            COUNT('size', win2, AS('count')),
            SUM('size', win2, AS('sum')),
            AVG('size', win2, AS('avg')),
            MIN('size', win2, AS('min')),
            MAX('size', win2, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame2 = `ROWS UNBOUNDED PRECEDING`
        const _win2 = `OVER(PARTITION BY city ORDER BY name, size ${ _frame2 })`

        const expected2 =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win2 }  AS count,
            SUM(size) ${ _win2 }  AS sum,
            AVG(size) ${ _win2 }  AS avg,
            MIN(size) ${ _win2 }  AS min,
            MAX(size) ${ _win2 }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();


        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
        expect(res).toEqual(res2);
        expect(res2).toEqual(expected2);

    });

    it(`3`, () => {
        const table = `
+----------+------+------+-------+-----+--------------------+-----+-----+
| city     | name | size | count | sum |                avg | min | max |
+----------+------+------+-------+-----+--------------------+-----+-----+
| Berlin   | A    |    5 |     3 |   8 | 2.6666666666666665 |   1 |   5 |
| Berlin   | B    |    1 |     4 |  11 |               2.75 |   1 |   5 |
| Berlin   | B    |    2 |     5 |  18 |                3.6 |   1 |   7 |
| Berlin   | B    |    3 |     6 |  19 | 3.1666666666666665 |   1 |   7 |
| Berlin   | B    |    7 |     7 |  21 |                  3 |   1 |   7 |
| Berlin   | C    |    1 |     8 |  24 |                  3 |   1 |   7 |
| Berlin   | C    |    2 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                  3 |   1 |   7 |
| Budapest | B    | null |     1 |   5 |                  5 |   5 |   5 |
| Budapest | B    | null |     2 |  16 |                  8 |   5 |  11 |
| Budapest | B    |    5 |     2 |  16 |                  8 |   5 |  11 |
| Budapest | B    |   11 |     2 |  16 |                  8 |   5 |  11 |
| Budapest | C    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    |    3 |     3 |  19 |  6.333333333333333 |   3 |  11 |
| London   | A    |    8 |     3 |  24 |                  8 |   8 |   8 |
| London   | A    |    8 |     4 |  33 |               8.25 |   8 |   9 |
| London   | A    |    8 |     4 |  33 |               8.25 |   8 |   9 |
| London   | A    |    9 |     5 |  35 |                  7 |   2 |   9 |
| London   | C    | null |     5 |  35 |                  7 |   2 |   9 |
| London   | C    |    2 |     5 |  35 |                  7 |   2 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |    3 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |   22 |     3 |  27 |                  9 |   2 |  22 |
| Paris    | A    | null |     2 |   8 |                  4 |   4 |   4 |
| Paris    | A    |    4 |     3 |  12 |                  4 |   4 |   4 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    7 |     4 |  19 |               4.75 |   4 |   7 |
| Rome     | A    | null |     1 |   4 |                  4 |   4 |   4 |
| Rome     | A    |    4 |     2 |   6 |                  3 |   2 |   4 |
| Rome     | B    | null |     3 |  16 |  5.333333333333333 |   2 |  10 |
| Rome     | C    |    2 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | C    |   10 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | C    |   11 |     4 |  27 |               6.75 |   2 |  11 |
+----------+------+------+-------+-----+--------------------+-----+-----+
36 rows selected`;

        const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, 2, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`4`, () => {
        const table = `
+----------+------+------+-------+-----+-------------------+-----+-----+
| city     | name | size | count | sum |               avg | min | max |
+----------+------+------+-------+-----+-------------------+-----+-----+
| Berlin   | A    |    5 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    1 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    2 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    7 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    1 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    2 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Budapest | B    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    |    5 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    |   11 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | C    |    3 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| London   | A    |    8 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    9 |     5 |  35 |                 7 |   2 |   9 |
| London   | C    | null |     5 |  35 |                 7 |   2 |   9 |
| London   | C    |    2 |     5 |  35 |                 7 |   2 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                 9 |   2 |  22 |
| Madrid   | B    |    3 |     3 |  27 |                 9 |   2 |  22 |
| Madrid   | B    |   22 |     3 |  27 |                 9 |   2 |  22 |
| Paris    | A    | null |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    7 |     4 |  19 |              4.75 |   4 |   7 |
| Rome     | A    | null |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | A    |    4 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | B    | null |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |    2 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |   10 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |   11 |     4 |  27 |              6.75 |   2 |  11 |
+----------+------+------+-------+-----+-------------------+-----+-----+
36 rows selected`;

        const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`5`, () => {
        const table = `
+----------+------+------+-------+------+--------------------+------+------+
| city     | name | size | count |  sum |                avg |  min |  max |
+----------+------+------+-------+------+--------------------+------+------+
| Berlin   | A    |    5 |     0 | null |               null | null | null |
| Berlin   | B    |    1 |     1 |    5 |                  5 |    5 |    5 |
| Berlin   | B    |    2 |     2 |    6 |                  3 |    1 |    5 |
| Berlin   | B    |    3 |     3 |    8 | 2.6666666666666665 |    1 |    5 |
| Berlin   | B    |    7 |     3 |    6 |                  2 |    1 |    3 |
| Berlin   | C    |    1 |     3 |   12 |                  4 |    2 |    7 |
| Berlin   | C    |    2 |     3 |   11 | 3.6666666666666665 |    1 |    7 |
| Berlin   | C    |    3 |     3 |   10 | 3.3333333333333335 |    1 |    7 |
| Berlin   | C    |    3 |     3 |    6 |                  2 |    1 |    3 |
| Budapest | B    | null |     0 | null |               null | null | null |
| Budapest | B    | null |     0 | null |               null | null | null |
| Budapest | B    |    5 |     0 | null |               null | null | null |
| Budapest | B    |   11 |     1 |    5 |                  5 |    5 |    5 |
| Budapest | C    | null |     2 |   16 |                  8 |    5 |   11 |
| Budapest | C    | null |     2 |   16 |                  8 |    5 |   11 |
| Budapest | C    |    3 |     1 |   11 |                 11 |   11 |   11 |
| London   | A    |    8 |     0 | null |               null | null | null |
| London   | A    |    8 |     1 |    8 |                  8 |    8 |    8 |
| London   | A    |    8 |     2 |   16 |                  8 |    8 |    8 |
| London   | A    |    9 |     3 |   24 |                  8 |    8 |    8 |
| London   | C    | null |     3 |   25 |  8.333333333333334 |    8 |    9 |
| London   | C    |    2 |     2 |   17 |                8.5 |    8 |    9 |
| Madrid   | B    |    2 |     0 | null |               null | null | null |
| Madrid   | B    |    3 |     1 |    2 |                  2 |    2 |    2 |
| Madrid   | B    |   22 |     2 |    5 |                2.5 |    2 |    3 |
| Paris    | A    | null |     0 | null |               null | null | null |
| Paris    | A    |    4 |     0 | null |               null | null | null |
| Paris    | A    |    4 |     1 |    4 |                  4 |    4 |    4 |
| Paris    | A    |    4 |     2 |    8 |                  4 |    4 |    4 |
| Paris    | A    |    7 |     3 |   12 |                  4 |    4 |    4 |
| Rome     | A    | null |     0 | null |               null | null | null |
| Rome     | A    |    4 |     0 | null |               null | null | null |
| Rome     | B    | null |     1 |    4 |                  4 |    4 |    4 |
| Rome     | C    |    2 |     1 |    4 |                  4 |    4 |    4 |
| Rome     | C    |   10 |     2 |    6 |                  3 |    2 |    4 |
| Rome     | C    |   11 |     2 |   12 |                  6 |    2 |   10 |
+----------+------+------+-------+------+--------------------+------+------+
36 rows selected`;

        const frame = ROWS_BETWEEN(3, PRECEDING, 1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`5a`, () => {
        const table = `
+----------+------+------+-------+------+------+------+------+
| city     | name | size | count |  sum |  avg |  min |  max |
+----------+------+------+-------+------+------+------+------+
| Berlin   | A    |    5 |     0 | null | null | null | null |
| Berlin   | B    |    1 |     0 | null | null | null | null |
| Berlin   | B    |    2 |     0 | null | null | null | null |
| Berlin   | B    |    3 |     0 | null | null | null | null |
| Berlin   | B    |    7 |     0 | null | null | null | null |
| Berlin   | C    |    1 |     0 | null | null | null | null |
| Berlin   | C    |    2 |     0 | null | null | null | null |
| Berlin   | C    |    3 |     0 | null | null | null | null |
| Berlin   | C    |    3 |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    |    5 |     0 | null | null | null | null |
| Budapest | B    |   11 |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    |    3 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    9 |     0 | null | null | null | null |
| London   | C    | null |     0 | null | null | null | null |
| London   | C    |    2 |     0 | null | null | null | null |
| Madrid   | B    |    2 |     0 | null | null | null | null |
| Madrid   | B    |    3 |     0 | null | null | null | null |
| Madrid   | B    |   22 |     0 | null | null | null | null |
| Paris    | A    | null |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    7 |     0 | null | null | null | null |
| Rome     | A    | null |     0 | null | null | null | null |
| Rome     | A    |    4 |     0 | null | null | null | null |
| Rome     | B    | null |     0 | null | null | null | null |
| Rome     | C    |    2 |     0 | null | null | null | null |
| Rome     | C    |   10 |     0 | null | null | null | null |
| Rome     | C    |   11 |     0 | null | null | null | null |
+----------+------+------+-------+------+------+------+------+
36 rows selected`;

        const frame = ROWS_BETWEEN(1, PRECEDING, 3, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`6`, () => {
        const table = `
+----------+------+------+-------+------+--------------------+------+------+
| city     | name | size | count |  sum |                avg |  min |  max |
+----------+------+------+-------+------+--------------------+------+------+
| Berlin   | A    |    5 |     1 |    5 |                  5 |    5 |    5 |
| Berlin   | B    |    1 |     2 |    6 |                  3 |    1 |    5 |
| Berlin   | B    |    2 |     3 |    8 | 2.6666666666666665 |    1 |    5 |
| Berlin   | B    |    3 |     4 |   11 |               2.75 |    1 |    5 |
| Berlin   | B    |    7 |     4 |   13 |               3.25 |    1 |    7 |
| Berlin   | C    |    1 |     4 |   13 |               3.25 |    1 |    7 |
| Berlin   | C    |    2 |     4 |   13 |               3.25 |    1 |    7 |
| Berlin   | C    |    3 |     4 |   13 |               3.25 |    1 |    7 |
| Berlin   | C    |    3 |     4 |    9 |               2.25 |    1 |    3 |
| Budapest | B    | null |     0 | null |               null | null | null |
| Budapest | B    | null |     0 | null |               null | null | null |
| Budapest | B    |    5 |     1 |    5 |                  5 |    5 |    5 |
| Budapest | B    |   11 |     2 |   16 |                  8 |    5 |   11 |
| Budapest | C    | null |     2 |   16 |                  8 |    5 |   11 |
| Budapest | C    | null |     2 |   16 |                  8 |    5 |   11 |
| Budapest | C    |    3 |     2 |   14 |                  7 |    3 |   11 |
| London   | A    |    8 |     1 |    8 |                  8 |    8 |    8 |
| London   | A    |    8 |     2 |   16 |                  8 |    8 |    8 |
| London   | A    |    8 |     3 |   24 |                  8 |    8 |    8 |
| London   | A    |    9 |     4 |   33 |               8.25 |    8 |    9 |
| London   | C    | null |     3 |   25 |  8.333333333333334 |    8 |    9 |
| London   | C    |    2 |     3 |   19 |  6.333333333333333 |    2 |    9 |
| Madrid   | B    |    2 |     1 |    2 |                  2 |    2 |    2 |
| Madrid   | B    |    3 |     2 |    5 |                2.5 |    2 |    3 |
| Madrid   | B    |   22 |     3 |   27 |                  9 |    2 |   22 |
| Paris    | A    | null |     0 | null |               null | null | null |
| Paris    | A    |    4 |     1 |    4 |                  4 |    4 |    4 |
| Paris    | A    |    4 |     2 |    8 |                  4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                  4 |    4 |    4 |
| Paris    | A    |    7 |     4 |   19 |               4.75 |    4 |    7 |
| Rome     | A    | null |     0 | null |               null | null | null |
| Rome     | A    |    4 |     1 |    4 |                  4 |    4 |    4 |
| Rome     | B    | null |     1 |    4 |                  4 |    4 |    4 |
| Rome     | C    |    2 |     2 |    6 |                  3 |    2 |    4 |
| Rome     | C    |   10 |     3 |   16 |  5.333333333333333 |    2 |   10 |
| Rome     | C    |   11 |     3 |   23 |  7.666666666666667 |    2 |   11 |
+----------+------+------+-------+------+--------------------+------+------+
36 rows selected`;

        const frame = ROWS_BETWEEN(3, PRECEDING, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN 3 PRECEDING AND CURRENT ROW`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        const frame2 = ROWS(3, PRECEDING)
        const win2 = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame2)
        
        const res2 =
        SELECT('city', 'name', 'size',
            COUNT('size', win2, AS('count')),
            SUM('size', win2, AS('sum')),
            AVG('size', win2, AS('avg')),
            MIN('size', win2, AS('min')),
            MAX('size', win2, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame2 = `ROWS 3 PRECEDING`
        const _win2 = `OVER(PARTITION BY city ORDER BY name, size ${ _frame2 })`

        const expected2 =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win2 }  AS count,
            SUM(size) ${ _win2 }  AS sum,
            AVG(size) ${ _win2 }  AS avg,
            MIN(size) ${ _win2 }  AS min,
            MAX(size) ${ _win2 }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();


        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
        expect(res).toEqual(res2);
        expect(res2).toEqual(expected2);
    });

    it(`7`, () => {
        const table = `
+----------+------+------+-------+-----+--------------------+-----+-----+
| city     | name | size | count | sum |                avg | min | max |
+----------+------+------+-------+-----+--------------------+-----+-----+
| Berlin   | A    |    5 |     3 |   8 | 2.6666666666666665 |   1 |   5 |
| Berlin   | B    |    1 |     4 |  11 |               2.75 |   1 |   5 |
| Berlin   | B    |    2 |     5 |  18 |                3.6 |   1 |   7 |
| Berlin   | B    |    3 |     5 |  14 |                2.8 |   1 |   7 |
| Berlin   | B    |    7 |     5 |  15 |                  3 |   1 |   7 |
| Berlin   | C    |    1 |     5 |  16 |                3.2 |   1 |   7 |
| Berlin   | C    |    2 |     5 |  16 |                3.2 |   1 |   7 |
| Berlin   | C    |    3 |     4 |   9 |               2.25 |   1 |   3 |
| Berlin   | C    |    3 |     3 |   8 | 2.6666666666666665 |   2 |   3 |
| Budapest | B    | null |     1 |   5 |                  5 |   5 |   5 |
| Budapest | B    | null |     2 |  16 |                  8 |   5 |  11 |
| Budapest | B    |    5 |     2 |  16 |                  8 |   5 |  11 |
| Budapest | B    |   11 |     2 |  16 |                  8 |   5 |  11 |
| Budapest | C    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     2 |  14 |                  7 |   3 |  11 |
| Budapest | C    |    3 |     1 |   3 |                  3 |   3 |   3 |
| London   | A    |    8 |     3 |  24 |                  8 |   8 |   8 |
| London   | A    |    8 |     4 |  33 |               8.25 |   8 |   9 |
| London   | A    |    8 |     4 |  33 |               8.25 |   8 |   9 |
| London   | A    |    9 |     4 |  27 |               6.75 |   2 |   9 |
| London   | C    | null |     3 |  19 |  6.333333333333333 |   2 |   9 |
| London   | C    |    2 |     2 |  11 |                5.5 |   2 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |    3 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |   22 |     3 |  27 |                  9 |   2 |  22 |
| Paris    | A    | null |     2 |   8 |                  4 |   4 |   4 |
| Paris    | A    |    4 |     3 |  12 |                  4 |   4 |   4 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    7 |     3 |  15 |                  5 |   4 |   7 |
| Rome     | A    | null |     1 |   4 |                  4 |   4 |   4 |
| Rome     | A    |    4 |     2 |   6 |                  3 |   2 |   4 |
| Rome     | B    | null |     3 |  16 |  5.333333333333333 |   2 |  10 |
| Rome     | C    |    2 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | C    |   10 |     3 |  23 |  7.666666666666667 |   2 |  11 |
| Rome     | C    |   11 |     3 |  23 |  7.666666666666667 |   2 |  11 |
+----------+------+------+-------+-----+--------------------+-----+-----+
36 rows selected`;

        const frame = ROWS_BETWEEN(2, PRECEDING, 2, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`8`, () => {
        const table = `
+----------+------+------+-------+-----+--------------------+-----+-----+
| city     | name | size | count | sum |                avg | min | max |
+----------+------+------+-------+-----+--------------------+-----+-----+
| Berlin   | A    |    5 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | B    |    1 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | B    |    2 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | B    |    3 |     8 |  22 |               2.75 |   1 |   7 |
| Berlin   | B    |    7 |     7 |  21 |                  3 |   1 |   7 |
| Berlin   | C    |    1 |     6 |  19 | 3.1666666666666665 |   1 |   7 |
| Berlin   | C    |    2 |     5 |  16 |                3.2 |   1 |   7 |
| Berlin   | C    |    3 |     4 |   9 |               2.25 |   1 |   3 |
| Berlin   | C    |    3 |     3 |   8 | 2.6666666666666665 |   2 |   3 |
| Budapest | B    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    |    5 |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    |   11 |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     2 |  14 |                  7 |   3 |  11 |
| Budapest | C    |    3 |     1 |   3 |                  3 |   3 |   3 |
| London   | A    |    8 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    9 |     4 |  27 |               6.75 |   2 |   9 |
| London   | C    | null |     3 |  19 |  6.333333333333333 |   2 |   9 |
| London   | C    |    2 |     2 |  11 |                5.5 |   2 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |    3 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |   22 |     3 |  27 |                  9 |   2 |  22 |
| Paris    | A    | null |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    7 |     3 |  15 |                  5 |   4 |   7 |
| Rome     | A    | null |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | A    |    4 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | B    | null |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | C    |    2 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | C    |   10 |     3 |  23 |  7.666666666666667 |   2 |  11 |
| Rome     | C    |   11 |     3 |  23 |  7.666666666666667 |   2 |  11 |
+----------+------+------+-------+-----+--------------------+-----+-----+
36 rows selected`;

        const frame = ROWS_BETWEEN(2, PRECEDING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`9`, () => {
        const table = `
+----------+------+------+-------+------+------+------+------+
| city     | name | size | count |  sum |  avg |  min |  max |
+----------+------+------+-------+------+------+------+------+
| Berlin   | A    |    5 |     1 |    5 |    5 |    5 |    5 |
| Berlin   | B    |    1 |     1 |    1 |    1 |    1 |    1 |
| Berlin   | B    |    2 |     1 |    2 |    2 |    2 |    2 |
| Berlin   | B    |    3 |     1 |    3 |    3 |    3 |    3 |
| Berlin   | B    |    7 |     1 |    7 |    7 |    7 |    7 |
| Berlin   | C    |    1 |     1 |    1 |    1 |    1 |    1 |
| Berlin   | C    |    2 |     1 |    2 |    2 |    2 |    2 |
| Berlin   | C    |    3 |     1 |    3 |    3 |    3 |    3 |
| Berlin   | C    |    3 |     1 |    3 |    3 |    3 |    3 |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    |    5 |     1 |    5 |    5 |    5 |    5 |
| Budapest | B    |   11 |     1 |   11 |   11 |   11 |   11 |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    |    3 |     1 |    3 |    3 |    3 |    3 |
| London   | A    |    8 |     1 |    8 |    8 |    8 |    8 |
| London   | A    |    8 |     1 |    8 |    8 |    8 |    8 |
| London   | A    |    8 |     1 |    8 |    8 |    8 |    8 |
| London   | A    |    9 |     1 |    9 |    9 |    9 |    9 |
| London   | C    | null |     0 | null | null | null | null |
| London   | C    |    2 |     1 |    2 |    2 |    2 |    2 |
| Madrid   | B    |    2 |     1 |    2 |    2 |    2 |    2 |
| Madrid   | B    |    3 |     1 |    3 |    3 |    3 |    3 |
| Madrid   | B    |   22 |     1 |   22 |   22 |   22 |   22 |
| Paris    | A    | null |     0 | null | null | null | null |
| Paris    | A    |    4 |     1 |    4 |    4 |    4 |    4 |
| Paris    | A    |    4 |     1 |    4 |    4 |    4 |    4 |
| Paris    | A    |    4 |     1 |    4 |    4 |    4 |    4 |
| Paris    | A    |    7 |     1 |    7 |    7 |    7 |    7 |
| Rome     | A    | null |     0 | null | null | null | null |
| Rome     | A    |    4 |     1 |    4 |    4 |    4 |    4 |
| Rome     | B    | null |     0 | null | null | null | null |
| Rome     | C    |    2 |     1 |    2 |    2 |    2 |    2 |
| Rome     | C    |   10 |     1 |   10 |   10 |   10 |   10 |
| Rome     | C    |   11 |     1 |   11 |   11 |   11 |   11 |
+----------+------+------+-------+------+------+------+------+
36 rows selected`;

        const frame = ROWS_BETWEEN(CURRENT_ROW, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN CURRENT ROW AND CURRENT ROW`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`10`, () => {
        const table = `
+----------+------+------+-------+-----+--------------------+-----+-----+
| city     | name | size | count | sum |                avg | min | max |
+----------+------+------+-------+-----+--------------------+-----+-----+
| Berlin   | A    |    5 |     3 |   8 | 2.6666666666666665 |   1 |   5 |
| Berlin   | B    |    1 |     3 |   6 |                  2 |   1 |   3 |
| Berlin   | B    |    2 |     3 |  12 |                  4 |   2 |   7 |
| Berlin   | B    |    3 |     3 |  11 | 3.6666666666666665 |   1 |   7 |
| Berlin   | B    |    7 |     3 |  10 | 3.3333333333333335 |   1 |   7 |
| Berlin   | C    |    1 |     3 |   6 |                  2 |   1 |   3 |
| Berlin   | C    |    2 |     3 |   8 | 2.6666666666666665 |   2 |   3 |
| Berlin   | C    |    3 |     2 |   6 |                  3 |   3 |   3 |
| Berlin   | C    |    3 |     1 |   3 |                  3 |   3 |   3 |
| Budapest | B    | null |     1 |   5 |                  5 |   5 |   5 |
| Budapest | B    | null |     2 |  16 |                  8 |   5 |  11 |
| Budapest | B    |    5 |     2 |  16 |                  8 |   5 |  11 |
| Budapest | B    |   11 |     1 |  11 |                 11 |  11 |  11 |
| Budapest | C    | null |     1 |   3 |                  3 |   3 |   3 |
| Budapest | C    | null |     1 |   3 |                  3 |   3 |   3 |
| Budapest | C    |    3 |     1 |   3 |                  3 |   3 |   3 |
| London   | A    |    8 |     3 |  24 |                  8 |   8 |   8 |
| London   | A    |    8 |     3 |  25 |  8.333333333333334 |   8 |   9 |
| London   | A    |    8 |     2 |  17 |                8.5 |   8 |   9 |
| London   | A    |    9 |     2 |  11 |                5.5 |   2 |   9 |
| London   | C    | null |     1 |   2 |                  2 |   2 |   2 |
| London   | C    |    2 |     1 |   2 |                  2 |   2 |   2 |
| Madrid   | B    |    2 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |    3 |     2 |  25 |               12.5 |   3 |  22 |
| Madrid   | B    |   22 |     1 |  22 |                 22 |  22 |  22 |
| Paris    | A    | null |     2 |   8 |                  4 |   4 |   4 |
| Paris    | A    |    4 |     3 |  12 |                  4 |   4 |   4 |
| Paris    | A    |    4 |     3 |  15 |                  5 |   4 |   7 |
| Paris    | A    |    4 |     2 |  11 |                5.5 |   4 |   7 |
| Paris    | A    |    7 |     1 |   7 |                  7 |   7 |   7 |
| Rome     | A    | null |     1 |   4 |                  4 |   4 |   4 |
| Rome     | A    |    4 |     2 |   6 |                  3 |   2 |   4 |
| Rome     | B    | null |     2 |  12 |                  6 |   2 |  10 |
| Rome     | C    |    2 |     3 |  23 |  7.666666666666667 |   2 |  11 |
| Rome     | C    |   10 |     2 |  21 |               10.5 |  10 |  11 |
| Rome     | C    |   11 |     1 |  11 |                 11 |  11 |  11 |
+----------+------+------+-------+-----+--------------------+-----+-----+
36 rows selected`;

        const frame = ROWS_BETWEEN(CURRENT_ROW, 2, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        const frame2 = ROWS(2, FOLLOWING)
        const win2 = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame2)
        
        const res2 =
        SELECT('city', 'name', 'size',
            COUNT('size', win2, AS('count')),
            SUM('size', win2, AS('sum')),
            AVG('size', win2, AS('avg')),
            MIN('size', win2, AS('min')),
            MAX('size', win2, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
        expect(res).toEqual(res2);
    });

    it(`11`, () => {
        const table = `
+----------+------+------+-------+-----+--------------------+-----+-----+
| city     | name | size | count | sum |                avg | min | max |
+----------+------+------+-------+-----+--------------------+-----+-----+
| Berlin   | A    |    5 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | B    |    1 |     8 |  22 |               2.75 |   1 |   7 |
| Berlin   | B    |    2 |     7 |  21 |                  3 |   1 |   7 |
| Berlin   | B    |    3 |     6 |  19 | 3.1666666666666665 |   1 |   7 |
| Berlin   | B    |    7 |     5 |  16 |                3.2 |   1 |   7 |
| Berlin   | C    |    1 |     4 |   9 |               2.25 |   1 |   3 |
| Berlin   | C    |    2 |     3 |   8 | 2.6666666666666665 |   2 |   3 |
| Berlin   | C    |    3 |     2 |   6 |                  3 |   3 |   3 |
| Berlin   | C    |    3 |     1 |   3 |                  3 |   3 |   3 |
| Budapest | B    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    |    5 |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    |   11 |     2 |  14 |                  7 |   3 |  11 |
| Budapest | C    | null |     1 |   3 |                  3 |   3 |   3 |
| Budapest | C    | null |     1 |   3 |                  3 |   3 |   3 |
| Budapest | C    |    3 |     1 |   3 |                  3 |   3 |   3 |
| London   | A    |    8 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    8 |     4 |  27 |               6.75 |   2 |   9 |
| London   | A    |    8 |     3 |  19 |  6.333333333333333 |   2 |   9 |
| London   | A    |    9 |     2 |  11 |                5.5 |   2 |   9 |
| London   | C    | null |     1 |   2 |                  2 |   2 |   2 |
| London   | C    |    2 |     1 |   2 |                  2 |   2 |   2 |
| Madrid   | B    |    2 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |    3 |     2 |  25 |               12.5 |   3 |  22 |
| Madrid   | B    |   22 |     1 |  22 |                 22 |  22 |  22 |
| Paris    | A    | null |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     3 |  15 |                  5 |   4 |   7 |
| Paris    | A    |    4 |     2 |  11 |                5.5 |   4 |   7 |
| Paris    | A    |    7 |     1 |   7 |                  7 |   7 |   7 |
| Rome     | A    | null |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | A    |    4 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | B    | null |     3 |  23 |  7.666666666666667 |   2 |  11 |
| Rome     | C    |    2 |     3 |  23 |  7.666666666666667 |   2 |  11 |
| Rome     | C    |   10 |     2 |  21 |               10.5 |  10 |  11 |
| Rome     | C    |   11 |     1 |  11 |                 11 |  11 |  11 |
+----------+------+------+-------+-----+--------------------+-----+-----+
36 rows selected`;

        const frame = ROWS_BETWEEN(CURRENT_ROW, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        const frame2 = ROWS(UNBOUNDED_FOLLOWING)
        const win2 = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame2)
        
        const res2 =
        SELECT('city', 'name', 'size',
            COUNT('size', win2, AS('count')),
            SUM('size', win2, AS('sum')),
            AVG('size', win2, AS('avg')),
            MIN('size', win2, AS('min')),
            MAX('size', win2, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
        expect(res).toEqual(res2);
    });

    it(`12`, () => {
        const table = `
+----------+------+------+-------+------+--------------------+------+------+
| city     | name | size | count |  sum |                avg |  min |  max |
+----------+------+------+-------+------+--------------------+------+------+
| Berlin   | A    |    5 |     3 |    6 |                  2 |    1 |    3 |
| Berlin   | B    |    1 |     3 |   12 |                  4 |    2 |    7 |
| Berlin   | B    |    2 |     3 |   11 | 3.6666666666666665 |    1 |    7 |
| Berlin   | B    |    3 |     3 |   10 | 3.3333333333333335 |    1 |    7 |
| Berlin   | B    |    7 |     3 |    6 |                  2 |    1 |    3 |
| Berlin   | C    |    1 |     3 |    8 | 2.6666666666666665 |    2 |    3 |
| Berlin   | C    |    2 |     2 |    6 |                  3 |    3 |    3 |
| Berlin   | C    |    3 |     1 |    3 |                  3 |    3 |    3 |
| Berlin   | C    |    3 |     0 | null |               null | null | null |
| Budapest | B    | null |     2 |   16 |                  8 |    5 |   11 |
| Budapest | B    | null |     2 |   16 |                  8 |    5 |   11 |
| Budapest | B    |    5 |     1 |   11 |                 11 |   11 |   11 |
| Budapest | B    |   11 |     1 |    3 |                  3 |    3 |    3 |
| Budapest | C    | null |     1 |    3 |                  3 |    3 |    3 |
| Budapest | C    | null |     1 |    3 |                  3 |    3 |    3 |
| Budapest | C    |    3 |     0 | null |               null | null | null |
| London   | A    |    8 |     3 |   25 |  8.333333333333334 |    8 |    9 |
| London   | A    |    8 |     2 |   17 |                8.5 |    8 |    9 |
| London   | A    |    8 |     2 |   11 |                5.5 |    2 |    9 |
| London   | A    |    9 |     1 |    2 |                  2 |    2 |    2 |
| London   | C    | null |     1 |    2 |                  2 |    2 |    2 |
| London   | C    |    2 |     0 | null |               null | null | null |
| Madrid   | B    |    2 |     2 |   25 |               12.5 |    3 |   22 |
| Madrid   | B    |    3 |     1 |   22 |                 22 |   22 |   22 |
| Madrid   | B    |   22 |     0 | null |               null | null | null |
| Paris    | A    | null |     3 |   12 |                  4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   15 |                  5 |    4 |    7 |
| Paris    | A    |    4 |     2 |   11 |                5.5 |    4 |    7 |
| Paris    | A    |    4 |     1 |    7 |                  7 |    7 |    7 |
| Paris    | A    |    7 |     0 | null |               null | null | null |
| Rome     | A    | null |     2 |    6 |                  3 |    2 |    4 |
| Rome     | A    |    4 |     2 |   12 |                  6 |    2 |   10 |
| Rome     | B    | null |     3 |   23 |  7.666666666666667 |    2 |   11 |
| Rome     | C    |    2 |     2 |   21 |               10.5 |   10 |   11 |
| Rome     | C    |   10 |     1 |   11 |                 11 |   11 |   11 |
| Rome     | C    |   11 |     0 | null |               null | null | null |
+----------+------+------+-------+------+--------------------+------+------+
36 rows selected`;

        const frame = ROWS_BETWEEN(1, FOLLOWING, 3, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`12a`, () => {
        const table = `
+----------+------+------+-------+------+------+------+------+
| city     | name | size | count |  sum |  avg |  min |  max |
+----------+------+------+-------+------+------+------+------+
| Berlin   | A    |    5 |     0 | null | null | null | null |
| Berlin   | B    |    1 |     0 | null | null | null | null |
| Berlin   | B    |    2 |     0 | null | null | null | null |
| Berlin   | B    |    3 |     0 | null | null | null | null |
| Berlin   | B    |    7 |     0 | null | null | null | null |
| Berlin   | C    |    1 |     0 | null | null | null | null |
| Berlin   | C    |    2 |     0 | null | null | null | null |
| Berlin   | C    |    3 |     0 | null | null | null | null |
| Berlin   | C    |    3 |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    |    5 |     0 | null | null | null | null |
| Budapest | B    |   11 |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    |    3 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    9 |     0 | null | null | null | null |
| London   | C    | null |     0 | null | null | null | null |
| London   | C    |    2 |     0 | null | null | null | null |
| Madrid   | B    |    2 |     0 | null | null | null | null |
| Madrid   | B    |    3 |     0 | null | null | null | null |
| Madrid   | B    |   22 |     0 | null | null | null | null |
| Paris    | A    | null |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    7 |     0 | null | null | null | null |
| Rome     | A    | null |     0 | null | null | null | null |
| Rome     | A    |    4 |     0 | null | null | null | null |
| Rome     | B    | null |     0 | null | null | null | null |
| Rome     | C    |    2 |     0 | null | null | null | null |
| Rome     | C    |   10 |     0 | null | null | null | null |
| Rome     | C    |   11 |     0 | null | null | null | null |
+----------+------+------+-------+------+------+------+------+
36 rows selected`;

        const frame = ROWS_BETWEEN(3, FOLLOWING, 1, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN 3 FOLLOWING AND 1 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });


    it(`13`, () => {
        const table = `
+----------+------+------+-------+------+--------------------+------+------+
| city     | name | size | count |  sum |                avg |  min |  max |
+----------+------+------+-------+------+--------------------+------+------+
| Berlin   | A    |    5 |     8 |   22 |               2.75 |    1 |    7 |
| Berlin   | B    |    1 |     7 |   21 |                  3 |    1 |    7 |
| Berlin   | B    |    2 |     6 |   19 | 3.1666666666666665 |    1 |    7 |
| Berlin   | B    |    3 |     5 |   16 |                3.2 |    1 |    7 |
| Berlin   | B    |    7 |     4 |    9 |               2.25 |    1 |    3 |
| Berlin   | C    |    1 |     3 |    8 | 2.6666666666666665 |    2 |    3 |
| Berlin   | C    |    2 |     2 |    6 |                  3 |    3 |    3 |
| Berlin   | C    |    3 |     1 |    3 |                  3 |    3 |    3 |
| Berlin   | C    |    3 |     0 | null |               null | null | null |
| Budapest | B    | null |     3 |   19 |  6.333333333333333 |    3 |   11 |
| Budapest | B    | null |     3 |   19 |  6.333333333333333 |    3 |   11 |
| Budapest | B    |    5 |     2 |   14 |                  7 |    3 |   11 |
| Budapest | B    |   11 |     1 |    3 |                  3 |    3 |    3 |
| Budapest | C    | null |     1 |    3 |                  3 |    3 |    3 |
| Budapest | C    | null |     1 |    3 |                  3 |    3 |    3 |
| Budapest | C    |    3 |     0 | null |               null | null | null |
| London   | A    |    8 |     4 |   27 |               6.75 |    2 |    9 |
| London   | A    |    8 |     3 |   19 |  6.333333333333333 |    2 |    9 |
| London   | A    |    8 |     2 |   11 |                5.5 |    2 |    9 |
| London   | A    |    9 |     1 |    2 |                  2 |    2 |    2 |
| London   | C    | null |     1 |    2 |                  2 |    2 |    2 |
| London   | C    |    2 |     0 | null |               null | null | null |
| Madrid   | B    |    2 |     2 |   25 |               12.5 |    3 |   22 |
| Madrid   | B    |    3 |     1 |   22 |                 22 |   22 |   22 |
| Madrid   | B    |   22 |     0 | null |               null | null | null |
| Paris    | A    | null |     4 |   19 |               4.75 |    4 |    7 |
| Paris    | A    |    4 |     3 |   15 |                  5 |    4 |    7 |
| Paris    | A    |    4 |     2 |   11 |                5.5 |    4 |    7 |
| Paris    | A    |    4 |     1 |    7 |                  7 |    7 |    7 |
| Paris    | A    |    7 |     0 | null |               null | null | null |
| Rome     | A    | null |     4 |   27 |               6.75 |    2 |   11 |
| Rome     | A    |    4 |     3 |   23 |  7.666666666666667 |    2 |   11 |
| Rome     | B    | null |     3 |   23 |  7.666666666666667 |    2 |   11 |
| Rome     | C    |    2 |     2 |   21 |               10.5 |   10 |   11 |
| Rome     | C    |   10 |     1 |   11 |                 11 |   11 |   11 |
| Rome     | C    |   11 |     0 | null |               null | null | null |
+----------+------+------+-------+------+--------------------+------+------+
36 rows selected`;

        const frame = ROWS_BETWEEN(1, FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY name, size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`14`, () => {
        const frame = ROWS_BETWEEN(1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('more frame boundaries expected (SELECT 4)'));
    });

    it(`15`, () => {
        const frame = ROWS_BETWEEN(1, 1, 1, 1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('less frame boundaries expected (SELECT 4)'));
    });

    it(`16`, () => {
        const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, UNBOUNDED_PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`17`, () => {
        const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });


    it(`18`, () => {
        const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, -1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('integer (>= 0) expected (SELECT 4)'));
    });

    it(`19`, () => {
        const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, 1.2, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('integer (>= 0) expected (SELECT 4)'));
    });

    it(`20`, () => {
        const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, 1, UNBOUNDED_PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`21`, () => {
        const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, 1, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`22`, () => {
        const frame = ROWS_BETWEEN(UNBOUNDED_PRECEDING, 1, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`23`, () => {
        const frame = ROWS_BETWEEN(UNBOUNDED_FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`24`, () => {
        const frame = ROWS_BETWEEN(1, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`25`, () => {
        const frame = ROWS_BETWEEN(-1, FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('integer (>= 0) expected (SELECT 4)'));
    });

    it(`26`, () => {
        const frame = ROWS_BETWEEN(1.2, FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('integer (>= 0) expected (SELECT 4)'));
    });

    it(`27`, () => {
        const frame = ROWS_BETWEEN(UNBOUNDED_FOLLOWING, 1, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`28`, () => {
        const frame = ROWS_BETWEEN(UNBOUNDED_FOLLOWING, UNBOUNDED_PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`29`, () => {
        const frame = ROWS_BETWEEN(1, UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`30`, () => {
        const frame = ROWS_BETWEEN(1, CURRENT_ROW, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`31`, () => {
        const frame = ROWS_BETWEEN(1, UNBOUNDED_FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`32`, () => {
        const frame = ROWS_BETWEEN(1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`33`, () => {
        const frame = ROWS_BETWEEN(null, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`34`, () => {
        const frame = ROWS_BETWEEN(1, null)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`35`, () => {
        const frame = ROWS_BETWEEN(1, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`36`, () => {
        const frame = ROWS_BETWEEN(CURRENT_ROW, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`37`, () => {
        const frame = ROWS_BETWEEN(1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`38`, () => {
        const frame = ROWS_BETWEEN(PRECEDING, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`39`, () => {
        const frame = ROWS_BETWEEN(1, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`40`, () => {
        const frame = ROWS_BETWEEN(FOLLOWING, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`41`, () => {
        const frame = ROWS_BETWEEN(PRECEDING, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`42`, () => {
        const frame = ROWS_BETWEEN(FOLLOWING, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`43`, () => {
        const frame = ROWS_BETWEEN(FOLLOWING, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`44`, () => {
        const frame = ROWS_BETWEEN(1, PRECEDING, 2)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`45`, () => {
        const frame = ROWS_BETWEEN(1, FOLLOWING, 2, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`46`, () => {
        const frame = ROWS_BETWEEN(1, FOLLOWING, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`47`, () => {
        const frame = ROWS_BETWEEN(FOLLOWING, 1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`48`, () => {
        const frame = ROWS_BETWEEN(FOLLOWING, 1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`49`, () => {
        const frame = ROWS_BETWEEN(PRECEDING, 1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`50`, () => {
        const frame = ROWS_BETWEEN(CURRENT_ROW, 1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`51`, () => {
        const frame = ROWS(null, null)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`52`, () => {
        const frame = ROWS(1, null)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`53`, () => {
        const frame = ROWS(null, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`54`, () => {
        const frame = ROWS(1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`55`, () => {
        const frame = ROWS(FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`56`, () => {
        const frame = ROWS(FOLLOWING, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`57`, () => {
        const frame = ROWS(FOLLOWING, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`58`, () => {
        const frame = ROWS(PRECEDING, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`59`, () => {
        const frame = ROWS(PRECEDING, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`60`, () => {
        const frame = ROWS(PRECEDING, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('name', 'size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });
});



///////////////////////////////////////////////////////////////////////////////////////////////////


describe('window functions -> (range_between + frames):', () => {
    it(`1`, () => {
        const table = `
+----------+------+------+-------+------+-------------------+------+------+
| city     | name | size | count |  sum |               avg |  min |  max |
+----------+------+------+-------+------+-------------------+------+------+
| Berlin   | B    |    1 |     0 | null |              null | null | null |
| Berlin   | C    |    1 |     0 | null |              null | null | null |
| Berlin   | B    |    2 |     2 |    2 |                 1 |    1 |    1 |
| Berlin   | C    |    2 |     2 |    2 |                 1 |    1 |    1 |
| Berlin   | B    |    3 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | C    |    3 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | C    |    3 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | A    |    5 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | B    |    7 |     8 |   20 |               2.5 |    1 |    5 |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    |    3 |     0 | null |              null | null | null |
| Budapest | B    |    5 |     1 |    3 |                 3 |    3 |    3 |
| Budapest | B    |   11 |     2 |    8 |                 4 |    3 |    5 |
| London   | C    | null |     0 | null |              null | null | null |
| London   | C    |    2 |     0 | null |              null | null | null |
| London   | A    |    8 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    8 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    8 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    9 |     4 |   26 |               6.5 |    2 |    8 |
| Madrid   | B    |    2 |     0 | null |              null | null | null |
| Madrid   | B    |    3 |     1 |    2 |                 2 |    2 |    2 |
| Madrid   | B    |   22 |     2 |    5 |               2.5 |    2 |    3 |
| Paris    | A    | null |     0 | null |              null | null | null |
| Paris    | A    |    4 |     0 | null |              null | null | null |
| Paris    | A    |    4 |     0 | null |              null | null | null |
| Paris    | A    |    4 |     0 | null |              null | null | null |
| Paris    | A    |    7 |     3 |   12 |                 4 |    4 |    4 |
| Rome     | A    | null |     0 | null |              null | null | null |
| Rome     | B    | null |     0 | null |              null | null | null |
| Rome     | C    |    2 |     0 | null |              null | null | null |
| Rome     | A    |    4 |     1 |    2 |                 2 |    2 |    2 |
| Rome     | C    |   10 |     2 |    6 |                 3 |    2 |    4 |
| Rome     | C    |   11 |     3 |   16 | 5.333333333333333 |    2 |   10 |
+----------+------+------+-------+------+-------------------+------+------+
36 rows selected`;

        const frame = RANGE_BETWEEN(UNBOUNDED_PRECEDING, 1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`2`, () => {
        const table = `
+----------+------+------+-------+------+-------------------+------+------+
| city     | name | size | count |  sum |               avg |  min |  max |
+----------+------+------+-------+------+-------------------+------+------+
| Berlin   | B    |    1 |     2 |    2 |                 1 |    1 |    1 |
| Berlin   | C    |    1 |     2 |    2 |                 1 |    1 |    1 |
| Berlin   | B    |    2 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | C    |    2 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | B    |    3 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | C    |    3 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | C    |    3 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | A    |    5 |     8 |   20 |               2.5 |    1 |    5 |
| Berlin   | B    |    7 |     9 |   27 |                 3 |    1 |    7 |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    |    3 |     1 |    3 |                 3 |    3 |    3 |
| Budapest | B    |    5 |     2 |    8 |                 4 |    3 |    5 |
| Budapest | B    |   11 |     3 |   19 | 6.333333333333333 |    3 |   11 |
| London   | C    | null |     0 | null |              null | null | null |
| London   | C    |    2 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    8 |     4 |   26 |               6.5 |    2 |    8 |
| London   | A    |    8 |     4 |   26 |               6.5 |    2 |    8 |
| London   | A    |    8 |     4 |   26 |               6.5 |    2 |    8 |
| London   | A    |    9 |     5 |   35 |                 7 |    2 |    9 |
| Madrid   | B    |    2 |     1 |    2 |                 2 |    2 |    2 |
| Madrid   | B    |    3 |     2 |    5 |               2.5 |    2 |    3 |
| Madrid   | B    |   22 |     3 |   27 |                 9 |    2 |   22 |
| Paris    | A    | null |     0 | null |              null | null | null |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    7 |     4 |   19 |              4.75 |    4 |    7 |
| Rome     | A    | null |     0 | null |              null | null | null |
| Rome     | B    | null |     0 | null |              null | null | null |
| Rome     | C    |    2 |     1 |    2 |                 2 |    2 |    2 |
| Rome     | A    |    4 |     2 |    6 |                 3 |    2 |    4 |
| Rome     | C    |   10 |     3 |   16 | 5.333333333333333 |    2 |   10 |
| Rome     | C    |   11 |     4 |   27 |              6.75 |    2 |   11 |
+----------+------+------+-------+------+-------------------+------+------+
36 rows selected`;

        const frame = RANGE_BETWEEN(UNBOUNDED_PRECEDING, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        const frame2 = RANGE(UNBOUNDED_PRECEDING)
        const win2 = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame2)
        
        const res2 =
        SELECT('city', 'name', 'size',
            COUNT('size', win2, AS('count')),
            SUM('size', win2, AS('sum')),
            AVG('size', win2, AS('avg')),
            MIN('size', win2, AS('min')),
            MAX('size', win2, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame2 = `RANGE UNBOUNDED PRECEDING`
        const _win2 = `OVER(PARTITION BY city ORDER BY size ${ _frame2 })`

        const expected2 =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win2 }  AS count,
            SUM(size) ${ _win2 }  AS sum,
            AVG(size) ${ _win2 }  AS avg,
            MIN(size) ${ _win2 }  AS min,
            MAX(size) ${ _win2 }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();


        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
        expect(res).toEqual(res2);
        expect(res2).toEqual(expected2);

    });

    it(`3`, () => {
        const table = `
+----------+------+------+-------+------+-------------------+------+------+
| city     | name | size | count |  sum |               avg |  min |  max |
+----------+------+------+-------+------+-------------------+------+------+
| Berlin   | B    |    1 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | C    |    1 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | B    |    2 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | C    |    2 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | B    |    3 |     8 |   20 |               2.5 |    1 |    5 |
| Berlin   | C    |    3 |     8 |   20 |               2.5 |    1 |    5 |
| Berlin   | C    |    3 |     8 |   20 |               2.5 |    1 |    5 |
| Berlin   | A    |    5 |     9 |   27 |                 3 |    1 |    7 |
| Berlin   | B    |    7 |     9 |   27 |                 3 |    1 |    7 |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    |    3 |     2 |    8 |                 4 |    3 |    5 |
| Budapest | B    |    5 |     2 |    8 |                 4 |    3 |    5 |
| Budapest | B    |   11 |     3 |   19 | 6.333333333333333 |    3 |   11 |
| London   | C    | null |     0 | null |              null | null | null |
| London   | C    |    2 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    8 |     5 |   35 |                 7 |    2 |    9 |
| London   | A    |    8 |     5 |   35 |                 7 |    2 |    9 |
| London   | A    |    8 |     5 |   35 |                 7 |    2 |    9 |
| London   | A    |    9 |     5 |   35 |                 7 |    2 |    9 |
| Madrid   | B    |    2 |     2 |    5 |               2.5 |    2 |    3 |
| Madrid   | B    |    3 |     2 |    5 |               2.5 |    2 |    3 |
| Madrid   | B    |   22 |     3 |   27 |                 9 |    2 |   22 |
| Paris    | A    | null |     0 | null |              null | null | null |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    7 |     4 |   19 |              4.75 |    4 |    7 |
| Rome     | A    | null |     0 | null |              null | null | null |
| Rome     | B    | null |     0 | null |              null | null | null |
| Rome     | C    |    2 |     2 |    6 |                 3 |    2 |    4 |
| Rome     | A    |    4 |     2 |    6 |                 3 |    2 |    4 |
| Rome     | C    |   10 |     4 |   27 |              6.75 |    2 |   11 |
| Rome     | C    |   11 |     4 |   27 |              6.75 |    2 |   11 |
+----------+------+------+-------+------+-------------------+------+------+
36 rows selected`;

        const frame = RANGE_BETWEEN(UNBOUNDED_PRECEDING, 2, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`4`, () => {
        const table = `
+----------+------+------+-------+-----+-------------------+-----+-----+
| city     | name | size | count | sum |               avg | min | max |
+----------+------+------+-------+-----+-------------------+-----+-----+
| Berlin   | B    |    1 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    1 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    2 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    2 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | A    |    5 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    7 |     9 |  27 |                 3 |   1 |   7 |
| Budapest | B    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | C    |    3 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    |    5 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    |   11 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| London   | C    | null |     5 |  35 |                 7 |   2 |   9 |
| London   | C    |    2 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    9 |     5 |  35 |                 7 |   2 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                 9 |   2 |  22 |
| Madrid   | B    |    3 |     3 |  27 |                 9 |   2 |  22 |
| Madrid   | B    |   22 |     3 |  27 |                 9 |   2 |  22 |
| Paris    | A    | null |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    7 |     4 |  19 |              4.75 |   4 |   7 |
| Rome     | A    | null |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | B    | null |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |    2 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | A    |    4 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |   10 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |   11 |     4 |  27 |              6.75 |   2 |  11 |
+----------+------+------+-------+-----+-------------------+-----+-----+
36 rows selected`;

        const frame = RANGE_BETWEEN(UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`5`, () => {
        const table = `
+----------+------+------+-------+------+------+------+------+
| city     | name | size | count |  sum |  avg |  min |  max |
+----------+------+------+-------+------+------+------+------+
| Berlin   | B    |    1 |     0 | null | null | null | null |
| Berlin   | C    |    1 |     0 | null | null | null | null |
| Berlin   | B    |    2 |     2 |    2 |    1 |    1 |    1 |
| Berlin   | C    |    2 |     2 |    2 |    1 |    1 |    1 |
| Berlin   | B    |    3 |     4 |    6 |  1.5 |    1 |    2 |
| Berlin   | C    |    3 |     4 |    6 |  1.5 |    1 |    2 |
| Berlin   | C    |    3 |     4 |    6 |  1.5 |    1 |    2 |
| Berlin   | A    |    5 |     5 |   13 |  2.6 |    2 |    3 |
| Berlin   | B    |    7 |     1 |    5 |    5 |    5 |    5 |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    |    3 |     0 | null | null | null | null |
| Budapest | B    |    5 |     1 |    3 |    3 |    3 |    3 |
| Budapest | B    |   11 |     0 | null | null | null | null |
| London   | C    | null |     0 | null | null | null | null |
| London   | C    |    2 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    9 |     3 |   24 |    8 |    8 |    8 |
| Madrid   | B    |    2 |     0 | null | null | null | null |
| Madrid   | B    |    3 |     1 |    2 |    2 |    2 |    2 |
| Madrid   | B    |   22 |     0 | null | null | null | null |
| Paris    | A    | null |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    7 |     3 |   12 |    4 |    4 |    4 |
| Rome     | A    | null |     0 | null | null | null | null |
| Rome     | B    | null |     0 | null | null | null | null |
| Rome     | C    |    2 |     0 | null | null | null | null |
| Rome     | A    |    4 |     1 |    2 |    2 |    2 |    2 |
| Rome     | C    |   10 |     0 | null | null | null | null |
| Rome     | C    |   11 |     1 |   10 |   10 |   10 |   10 |
+----------+------+------+-------+------+------+------+------+
36 rows selected`;

        const frame = RANGE_BETWEEN(3, PRECEDING, 1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`5a`, () => {
        const table = `
+----------+------+------+-------+------+------+------+------+
| city     | name | size | count |  sum |  avg |  min |  max |
+----------+------+------+-------+------+------+------+------+
| Berlin   | B    |    1 |     0 | null | null | null | null |
| Berlin   | C    |    1 |     0 | null | null | null | null |
| Berlin   | B    |    2 |     0 | null | null | null | null |
| Berlin   | C    |    2 |     0 | null | null | null | null |
| Berlin   | B    |    3 |     0 | null | null | null | null |
| Berlin   | C    |    3 |     0 | null | null | null | null |
| Berlin   | C    |    3 |     0 | null | null | null | null |
| Berlin   | A    |    5 |     0 | null | null | null | null |
| Berlin   | B    |    7 |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    |    3 |     0 | null | null | null | null |
| Budapest | B    |    5 |     0 | null | null | null | null |
| Budapest | B    |   11 |     0 | null | null | null | null |
| London   | C    | null |     0 | null | null | null | null |
| London   | C    |    2 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    9 |     0 | null | null | null | null |
| Madrid   | B    |    2 |     0 | null | null | null | null |
| Madrid   | B    |    3 |     0 | null | null | null | null |
| Madrid   | B    |   22 |     0 | null | null | null | null |
| Paris    | A    | null |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    7 |     0 | null | null | null | null |
| Rome     | A    | null |     0 | null | null | null | null |
| Rome     | B    | null |     0 | null | null | null | null |
| Rome     | C    |    2 |     0 | null | null | null | null |
| Rome     | A    |    4 |     0 | null | null | null | null |
| Rome     | C    |   10 |     0 | null | null | null | null |
| Rome     | C    |   11 |     0 | null | null | null | null |
+----------+------+------+-------+------+------+------+------+
36 rows selected`;

        const frame = RANGE_BETWEEN(1, PRECEDING, 3, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN 1 PRECEDING AND 3 PRECEDING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });


    it(`6`, () => {
        const table = `
+----------+------+------+-------+------+-------------------+------+------+
| city     | name | size | count |  sum |               avg |  min |  max |
+----------+------+------+-------+------+-------------------+------+------+
| Berlin   | B    |    1 |     2 |    2 |                 1 |    1 |    1 |
| Berlin   | C    |    1 |     2 |    2 |                 1 |    1 |    1 |
| Berlin   | B    |    2 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | C    |    2 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | B    |    3 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | C    |    3 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | C    |    3 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | A    |    5 |     6 |   18 |                 3 |    2 |    5 |
| Berlin   | B    |    7 |     2 |   12 |                 6 |    5 |    7 |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    |    3 |     1 |    3 |                 3 |    3 |    3 |
| Budapest | B    |    5 |     2 |    8 |                 4 |    3 |    5 |
| Budapest | B    |   11 |     1 |   11 |                11 |   11 |   11 |
| London   | C    | null |     0 | null |              null | null | null |
| London   | C    |    2 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    8 |     3 |   24 |                 8 |    8 |    8 |
| London   | A    |    8 |     3 |   24 |                 8 |    8 |    8 |
| London   | A    |    8 |     3 |   24 |                 8 |    8 |    8 |
| London   | A    |    9 |     4 |   33 |              8.25 |    8 |    9 |
| Madrid   | B    |    2 |     1 |    2 |                 2 |    2 |    2 |
| Madrid   | B    |    3 |     2 |    5 |               2.5 |    2 |    3 |
| Madrid   | B    |   22 |     1 |   22 |                22 |   22 |   22 |
| Paris    | A    | null |     0 | null |              null | null | null |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    7 |     4 |   19 |              4.75 |    4 |    7 |
| Rome     | A    | null |     0 | null |              null | null | null |
| Rome     | B    | null |     0 | null |              null | null | null |
| Rome     | C    |    2 |     1 |    2 |                 2 |    2 |    2 |
| Rome     | A    |    4 |     2 |    6 |                 3 |    2 |    4 |
| Rome     | C    |   10 |     1 |   10 |                10 |   10 |   10 |
| Rome     | C    |   11 |     2 |   21 |              10.5 |   10 |   11 |
+----------+------+------+-------+------+-------------------+------+------+
36 rows selected`;

        const frame = RANGE_BETWEEN(3, PRECEDING, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN 3 PRECEDING AND CURRENT ROW`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        const frame2 = RANGE(3, PRECEDING)
        const win2 = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame2)
        
        const res2 =
        SELECT('city', 'name', 'size',
            COUNT('size', win2, AS('count')),
            SUM('size', win2, AS('sum')),
            AVG('size', win2, AS('avg')),
            MIN('size', win2, AS('min')),
            MAX('size', win2, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame2 = `RANGE 3 PRECEDING`
        const _win2 = `OVER(PARTITION BY city ORDER BY size ${ _frame2 })`

        const expected2 =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win2 }  AS count,
            SUM(size) ${ _win2 }  AS sum,
            AVG(size) ${ _win2 }  AS avg,
            MIN(size) ${ _win2 }  AS min,
            MAX(size) ${ _win2 }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();


        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
        expect(res).toEqual(res2);
        expect(res2).toEqual(expected2);
    });

    it(`7`, () => {
        const table = `
+----------+------+------+-------+------+-------------------+------+------+
| city     | name | size | count |  sum |               avg |  min |  max |
+----------+------+------+-------+------+-------------------+------+------+
| Berlin   | B    |    1 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | C    |    1 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | B    |    2 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | C    |    2 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | B    |    3 |     8 |   20 |               2.5 |    1 |    5 |
| Berlin   | C    |    3 |     8 |   20 |               2.5 |    1 |    5 |
| Berlin   | C    |    3 |     8 |   20 |               2.5 |    1 |    5 |
| Berlin   | A    |    5 |     5 |   21 |               4.2 |    3 |    7 |
| Berlin   | B    |    7 |     2 |   12 |                 6 |    5 |    7 |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    |    3 |     2 |    8 |                 4 |    3 |    5 |
| Budapest | B    |    5 |     2 |    8 |                 4 |    3 |    5 |
| Budapest | B    |   11 |     1 |   11 |                11 |   11 |   11 |
| London   | C    | null |     0 | null |              null | null | null |
| London   | C    |    2 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    8 |     4 |   33 |              8.25 |    8 |    9 |
| London   | A    |    8 |     4 |   33 |              8.25 |    8 |    9 |
| London   | A    |    8 |     4 |   33 |              8.25 |    8 |    9 |
| London   | A    |    9 |     4 |   33 |              8.25 |    8 |    9 |
| Madrid   | B    |    2 |     2 |    5 |               2.5 |    2 |    3 |
| Madrid   | B    |    3 |     2 |    5 |               2.5 |    2 |    3 |
| Madrid   | B    |   22 |     1 |   22 |                22 |   22 |   22 |
| Paris    | A    | null |     0 | null |              null | null | null |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    7 |     1 |    7 |                 7 |    7 |    7 |
| Rome     | A    | null |     0 | null |              null | null | null |
| Rome     | B    | null |     0 | null |              null | null | null |
| Rome     | C    |    2 |     2 |    6 |                 3 |    2 |    4 |
| Rome     | A    |    4 |     2 |    6 |                 3 |    2 |    4 |
| Rome     | C    |   10 |     2 |   21 |              10.5 |   10 |   11 |
| Rome     | C    |   11 |     2 |   21 |              10.5 |   10 |   11 |
+----------+------+------+-------+------+-------------------+------+------+
36 rows selected`;

        const frame = RANGE_BETWEEN(2, PRECEDING, 2, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`8`, () => {
        const table = `
+----------+------+------+-------+-----+-------------------+-----+-----+
| city     | name | size | count | sum |               avg | min | max |
+----------+------+------+-------+-----+-------------------+-----+-----+
| Berlin   | B    |    1 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    1 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    2 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    2 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | A    |    5 |     5 |  21 |               4.2 |   3 |   7 |
| Berlin   | B    |    7 |     2 |  12 |                 6 |   5 |   7 |
| Budapest | B    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | C    |    3 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    |    5 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    |   11 |     1 |  11 |                11 |  11 |  11 |
| London   | C    | null |     5 |  35 |                 7 |   2 |   9 |
| London   | C    |    2 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     4 |  33 |              8.25 |   8 |   9 |
| London   | A    |    8 |     4 |  33 |              8.25 |   8 |   9 |
| London   | A    |    8 |     4 |  33 |              8.25 |   8 |   9 |
| London   | A    |    9 |     4 |  33 |              8.25 |   8 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                 9 |   2 |  22 |
| Madrid   | B    |    3 |     3 |  27 |                 9 |   2 |  22 |
| Madrid   | B    |   22 |     1 |  22 |                22 |  22 |  22 |
| Paris    | A    | null |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    7 |     1 |   7 |                 7 |   7 |   7 |
| Rome     | A    | null |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | B    | null |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |    2 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | A    |    4 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |   10 |     2 |  21 |              10.5 |  10 |  11 |
| Rome     | C    |   11 |     2 |  21 |              10.5 |  10 |  11 |
+----------+------+------+-------+-----+-------------------+-----+-----+
36 rows selected`;

        const frame = RANGE_BETWEEN(2, PRECEDING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`9`, () => {
        const table = `
+----------+------+------+-------+------+------+------+------+
| city     | name | size | count |  sum |  avg |  min |  max |
+----------+------+------+-------+------+------+------+------+
| Berlin   | B    |    1 |     2 |    2 |    1 |    1 |    1 |
| Berlin   | C    |    1 |     2 |    2 |    1 |    1 |    1 |
| Berlin   | B    |    2 |     2 |    4 |    2 |    2 |    2 |
| Berlin   | C    |    2 |     2 |    4 |    2 |    2 |    2 |
| Berlin   | B    |    3 |     3 |    9 |    3 |    3 |    3 |
| Berlin   | C    |    3 |     3 |    9 |    3 |    3 |    3 |
| Berlin   | C    |    3 |     3 |    9 |    3 |    3 |    3 |
| Berlin   | A    |    5 |     1 |    5 |    5 |    5 |    5 |
| Berlin   | B    |    7 |     1 |    7 |    7 |    7 |    7 |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    |    3 |     1 |    3 |    3 |    3 |    3 |
| Budapest | B    |    5 |     1 |    5 |    5 |    5 |    5 |
| Budapest | B    |   11 |     1 |   11 |   11 |   11 |   11 |
| London   | C    | null |     0 | null | null | null | null |
| London   | C    |    2 |     1 |    2 |    2 |    2 |    2 |
| London   | A    |    8 |     3 |   24 |    8 |    8 |    8 |
| London   | A    |    8 |     3 |   24 |    8 |    8 |    8 |
| London   | A    |    8 |     3 |   24 |    8 |    8 |    8 |
| London   | A    |    9 |     1 |    9 |    9 |    9 |    9 |
| Madrid   | B    |    2 |     1 |    2 |    2 |    2 |    2 |
| Madrid   | B    |    3 |     1 |    3 |    3 |    3 |    3 |
| Madrid   | B    |   22 |     1 |   22 |   22 |   22 |   22 |
| Paris    | A    | null |     0 | null | null | null | null |
| Paris    | A    |    4 |     3 |   12 |    4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |    4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |    4 |    4 |    4 |
| Paris    | A    |    7 |     1 |    7 |    7 |    7 |    7 |
| Rome     | A    | null |     0 | null | null | null | null |
| Rome     | B    | null |     0 | null | null | null | null |
| Rome     | C    |    2 |     1 |    2 |    2 |    2 |    2 |
| Rome     | A    |    4 |     1 |    4 |    4 |    4 |    4 |
| Rome     | C    |   10 |     1 |   10 |   10 |   10 |   10 |
| Rome     | C    |   11 |     1 |   11 |   11 |   11 |   11 |
+----------+------+------+-------+------+------+------+------+
36 rows selected`;

        const frame = RANGE_BETWEEN(CURRENT_ROW, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN CURRENT ROW AND CURRENT ROW`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`10`, () => {
        const table = `
+----------+------+------+-------+------+-------------------+------+------+
| city     | name | size | count |  sum |               avg |  min |  max |
+----------+------+------+-------+------+-------------------+------+------+
| Berlin   | B    |    1 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | C    |    1 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | B    |    2 |     5 |   13 |               2.6 |    2 |    3 |
| Berlin   | C    |    2 |     5 |   13 |               2.6 |    2 |    3 |
| Berlin   | B    |    3 |     4 |   14 |               3.5 |    3 |    5 |
| Berlin   | C    |    3 |     4 |   14 |               3.5 |    3 |    5 |
| Berlin   | C    |    3 |     4 |   14 |               3.5 |    3 |    5 |
| Berlin   | A    |    5 |     2 |   12 |                 6 |    5 |    7 |
| Berlin   | B    |    7 |     1 |    7 |                 7 |    7 |    7 |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    |    3 |     2 |    8 |                 4 |    3 |    5 |
| Budapest | B    |    5 |     1 |    5 |                 5 |    5 |    5 |
| Budapest | B    |   11 |     1 |   11 |                11 |   11 |   11 |
| London   | C    | null |     0 | null |              null | null | null |
| London   | C    |    2 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    8 |     4 |   33 |              8.25 |    8 |    9 |
| London   | A    |    8 |     4 |   33 |              8.25 |    8 |    9 |
| London   | A    |    8 |     4 |   33 |              8.25 |    8 |    9 |
| London   | A    |    9 |     1 |    9 |                 9 |    9 |    9 |
| Madrid   | B    |    2 |     2 |    5 |               2.5 |    2 |    3 |
| Madrid   | B    |    3 |     1 |    3 |                 3 |    3 |    3 |
| Madrid   | B    |   22 |     1 |   22 |                22 |   22 |   22 |
| Paris    | A    | null |     0 | null |              null | null | null |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    7 |     1 |    7 |                 7 |    7 |    7 |
| Rome     | A    | null |     0 | null |              null | null | null |
| Rome     | B    | null |     0 | null |              null | null | null |
| Rome     | C    |    2 |     2 |    6 |                 3 |    2 |    4 |
| Rome     | A    |    4 |     1 |    4 |                 4 |    4 |    4 |
| Rome     | C    |   10 |     2 |   21 |              10.5 |   10 |   11 |
| Rome     | C    |   11 |     1 |   11 |                11 |   11 |   11 |
+----------+------+------+-------+------+-------------------+------+------+
36 rows selected`;

        const frame = RANGE_BETWEEN(CURRENT_ROW, 2, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        const frame2 = RANGE(2, FOLLOWING)
        const win2 = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame2)
        
        const res2 =
        SELECT('city', 'name', 'size',
            COUNT('size', win2, AS('count')),
            SUM('size', win2, AS('sum')),
            AVG('size', win2, AS('avg')),
            MIN('size', win2, AS('min')),
            MAX('size', win2, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
        expect(res).toEqual(res2);
    });

    it(`11`, () => {
        const table = `
+----------+------+------+-------+-----+--------------------+-----+-----+
| city     | name | size | count | sum |                avg | min | max |
+----------+------+------+-------+-----+--------------------+-----+-----+
| Berlin   | B    |    1 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | C    |    1 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | B    |    2 |     7 |  25 | 3.5714285714285716 |   2 |   7 |
| Berlin   | C    |    2 |     7 |  25 | 3.5714285714285716 |   2 |   7 |
| Berlin   | B    |    3 |     5 |  21 |                4.2 |   3 |   7 |
| Berlin   | C    |    3 |     5 |  21 |                4.2 |   3 |   7 |
| Berlin   | C    |    3 |     5 |  21 |                4.2 |   3 |   7 |
| Berlin   | A    |    5 |     2 |  12 |                  6 |   5 |   7 |
| Berlin   | B    |    7 |     1 |   7 |                  7 |   7 |   7 |
| Budapest | B    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    |    3 |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    |    5 |     2 |  16 |                  8 |   5 |  11 |
| Budapest | B    |   11 |     1 |  11 |                 11 |  11 |  11 |
| London   | C    | null |     5 |  35 |                  7 |   2 |   9 |
| London   | C    |    2 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    8 |     4 |  33 |               8.25 |   8 |   9 |
| London   | A    |    8 |     4 |  33 |               8.25 |   8 |   9 |
| London   | A    |    8 |     4 |  33 |               8.25 |   8 |   9 |
| London   | A    |    9 |     1 |   9 |                  9 |   9 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |    3 |     2 |  25 |               12.5 |   3 |  22 |
| Madrid   | B    |   22 |     1 |  22 |                 22 |  22 |  22 |
| Paris    | A    | null |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    7 |     1 |   7 |                  7 |   7 |   7 |
| Rome     | A    | null |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | B    | null |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | C    |    2 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | A    |    4 |     3 |  25 |  8.333333333333334 |   4 |  11 |
| Rome     | C    |   10 |     2 |  21 |               10.5 |  10 |  11 |
| Rome     | C    |   11 |     1 |  11 |                 11 |  11 |  11 |
+----------+------+------+-------+-----+--------------------+-----+-----+
36 rows selected`;

        const frame = RANGE_BETWEEN(CURRENT_ROW, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        const frame2 = RANGE(UNBOUNDED_FOLLOWING)
        const win2 = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame2)
        
        const res2 =
        SELECT('city', 'name', 'size',
            COUNT('size', win2, AS('count')),
            SUM('size', win2, AS('sum')),
            AVG('size', win2, AS('avg')),
            MIN('size', win2, AS('min')),
            MAX('size', win2, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
        expect(res).toEqual(res2);
    });

    it(`12`, () => {
        const table = `
+----------+------+------+-------+------+------+------+------+
| city     | name | size | count |  sum |  avg |  min |  max |
+----------+------+------+-------+------+------+------+------+
| Berlin   | B    |    1 |     5 |   13 |  2.6 |    2 |    3 |
| Berlin   | C    |    1 |     5 |   13 |  2.6 |    2 |    3 |
| Berlin   | B    |    2 |     4 |   14 |  3.5 |    3 |    5 |
| Berlin   | C    |    2 |     4 |   14 |  3.5 |    3 |    5 |
| Berlin   | B    |    3 |     1 |    5 |    5 |    5 |    5 |
| Berlin   | C    |    3 |     1 |    5 |    5 |    5 |    5 |
| Berlin   | C    |    3 |     1 |    5 |    5 |    5 |    5 |
| Berlin   | A    |    5 |     1 |    7 |    7 |    7 |    7 |
| Berlin   | B    |    7 |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    |    3 |     1 |    5 |    5 |    5 |    5 |
| Budapest | B    |    5 |     0 | null | null | null | null |
| Budapest | B    |   11 |     0 | null | null | null | null |
| London   | C    | null |     0 | null | null | null | null |
| London   | C    |    2 |     0 | null | null | null | null |
| London   | A    |    8 |     1 |    9 |    9 |    9 |    9 |
| London   | A    |    8 |     1 |    9 |    9 |    9 |    9 |
| London   | A    |    8 |     1 |    9 |    9 |    9 |    9 |
| London   | A    |    9 |     0 | null | null | null | null |
| Madrid   | B    |    2 |     1 |    3 |    3 |    3 |    3 |
| Madrid   | B    |    3 |     0 | null | null | null | null |
| Madrid   | B    |   22 |     0 | null | null | null | null |
| Paris    | A    | null |     0 | null | null | null | null |
| Paris    | A    |    4 |     1 |    7 |    7 |    7 |    7 |
| Paris    | A    |    4 |     1 |    7 |    7 |    7 |    7 |
| Paris    | A    |    4 |     1 |    7 |    7 |    7 |    7 |
| Paris    | A    |    7 |     0 | null | null | null | null |
| Rome     | A    | null |     0 | null | null | null | null |
| Rome     | B    | null |     0 | null | null | null | null |
| Rome     | C    |    2 |     1 |    4 |    4 |    4 |    4 |
| Rome     | A    |    4 |     0 | null | null | null | null |
| Rome     | C    |   10 |     1 |   11 |   11 |   11 |   11 |
| Rome     | C    |   11 |     0 | null | null | null | null |
+----------+------+------+-------+------+------+------+------+
36 rows selected`;

        const frame = RANGE_BETWEEN(1, FOLLOWING, 3, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN 1 FOLLOWING AND 3 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`12a`, () => {
        const table = `
+----------+------+------+-------+------+------+------+------+
| city     | name | size | count |  sum |  avg |  min |  max |
+----------+------+------+-------+------+------+------+------+
| Berlin   | B    |    1 |     0 | null | null | null | null |
| Berlin   | C    |    1 |     0 | null | null | null | null |
| Berlin   | B    |    2 |     0 | null | null | null | null |
| Berlin   | C    |    2 |     0 | null | null | null | null |
| Berlin   | B    |    3 |     0 | null | null | null | null |
| Berlin   | C    |    3 |     0 | null | null | null | null |
| Berlin   | C    |    3 |     0 | null | null | null | null |
| Berlin   | A    |    5 |     0 | null | null | null | null |
| Berlin   | B    |    7 |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    |    3 |     0 | null | null | null | null |
| Budapest | B    |    5 |     0 | null | null | null | null |
| Budapest | B    |   11 |     0 | null | null | null | null |
| London   | C    | null |     0 | null | null | null | null |
| London   | C    |    2 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    9 |     0 | null | null | null | null |
| Madrid   | B    |    2 |     0 | null | null | null | null |
| Madrid   | B    |    3 |     0 | null | null | null | null |
| Madrid   | B    |   22 |     0 | null | null | null | null |
| Paris    | A    | null |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    7 |     0 | null | null | null | null |
| Rome     | A    | null |     0 | null | null | null | null |
| Rome     | B    | null |     0 | null | null | null | null |
| Rome     | C    |    2 |     0 | null | null | null | null |
| Rome     | A    |    4 |     0 | null | null | null | null |
| Rome     | C    |   10 |     0 | null | null | null | null |
| Rome     | C    |   11 |     0 | null | null | null | null |
+----------+------+------+-------+------+------+------+------+
36 rows selected`;

        const frame = RANGE_BETWEEN(3, FOLLOWING, 1, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN 3 FOLLOWING AND 1 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });


    it(`13`, () => {
        const table = `
+----------+------+------+-------+------+--------------------+------+------+
| city     | name | size | count |  sum |                avg |  min |  max |
+----------+------+------+-------+------+--------------------+------+------+
| Berlin   | B    |    1 |     7 |   25 | 3.5714285714285716 |    2 |    7 |
| Berlin   | C    |    1 |     7 |   25 | 3.5714285714285716 |    2 |    7 |
| Berlin   | B    |    2 |     5 |   21 |                4.2 |    3 |    7 |
| Berlin   | C    |    2 |     5 |   21 |                4.2 |    3 |    7 |
| Berlin   | B    |    3 |     2 |   12 |                  6 |    5 |    7 |
| Berlin   | C    |    3 |     2 |   12 |                  6 |    5 |    7 |
| Berlin   | C    |    3 |     2 |   12 |                  6 |    5 |    7 |
| Berlin   | A    |    5 |     1 |    7 |                  7 |    7 |    7 |
| Berlin   | B    |    7 |     0 | null |               null | null | null |
| Budapest | B    | null |     3 |   19 |  6.333333333333333 |    3 |   11 |
| Budapest | B    | null |     3 |   19 |  6.333333333333333 |    3 |   11 |
| Budapest | C    | null |     3 |   19 |  6.333333333333333 |    3 |   11 |
| Budapest | C    | null |     3 |   19 |  6.333333333333333 |    3 |   11 |
| Budapest | C    |    3 |     2 |   16 |                  8 |    5 |   11 |
| Budapest | B    |    5 |     1 |   11 |                 11 |   11 |   11 |
| Budapest | B    |   11 |     0 | null |               null | null | null |
| London   | C    | null |     5 |   35 |                  7 |    2 |    9 |
| London   | C    |    2 |     4 |   33 |               8.25 |    8 |    9 |
| London   | A    |    8 |     1 |    9 |                  9 |    9 |    9 |
| London   | A    |    8 |     1 |    9 |                  9 |    9 |    9 |
| London   | A    |    8 |     1 |    9 |                  9 |    9 |    9 |
| London   | A    |    9 |     0 | null |               null | null | null |
| Madrid   | B    |    2 |     2 |   25 |               12.5 |    3 |   22 |
| Madrid   | B    |    3 |     1 |   22 |                 22 |   22 |   22 |
| Madrid   | B    |   22 |     0 | null |               null | null | null |
| Paris    | A    | null |     4 |   19 |               4.75 |    4 |    7 |
| Paris    | A    |    4 |     1 |    7 |                  7 |    7 |    7 |
| Paris    | A    |    4 |     1 |    7 |                  7 |    7 |    7 |
| Paris    | A    |    4 |     1 |    7 |                  7 |    7 |    7 |
| Paris    | A    |    7 |     0 | null |               null | null | null |
| Rome     | A    | null |     4 |   27 |               6.75 |    2 |   11 |
| Rome     | B    | null |     4 |   27 |               6.75 |    2 |   11 |
| Rome     | C    |    2 |     3 |   25 |  8.333333333333334 |    4 |   11 |
| Rome     | A    |    4 |     2 |   21 |               10.5 |   10 |   11 |
| Rome     | C    |   10 |     1 |   11 |                 11 |   11 |   11 |
| Rome     | C    |   11 |     0 | null |               null | null | null |
+----------+------+------+-------+------+--------------------+------+------+
36 rows selected`;

        const frame = RANGE_BETWEEN(1, FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`14`, () => {
        const frame = RANGE_BETWEEN(1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('more frame boundaries expected (SELECT 4)'));
    });

    it(`15`, () => {
        const frame = RANGE_BETWEEN(1, 1, 1, 1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('less frame boundaries expected (SELECT 4)'));
    });

    it(`16`, () => {
        const frame = RANGE_BETWEEN(UNBOUNDED_PRECEDING, UNBOUNDED_PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`17`, () => {
        const frame = RANGE_BETWEEN(UNBOUNDED_PRECEDING, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });


    it(`18`, () => {
        const frame = RANGE_BETWEEN(UNBOUNDED_PRECEDING, -1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('number (>= 0) expected (SELECT 4)'));
    });

    it(`19`, () => {
        const frame = RANGE_BETWEEN(UNBOUNDED_PRECEDING, NaN, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`20`, () => {
        const frame = RANGE_BETWEEN(UNBOUNDED_PRECEDING, 1, UNBOUNDED_PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`21`, () => {
        const frame = RANGE_BETWEEN(UNBOUNDED_PRECEDING, 1, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`22`, () => {
        const frame = RANGE_BETWEEN(UNBOUNDED_PRECEDING, 1, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`23`, () => {
        const frame = RANGE_BETWEEN(UNBOUNDED_FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`24`, () => {
        const frame = RANGE_BETWEEN(1, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`25`, () => {
        const frame = RANGE_BETWEEN(-1, FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('number (>= 0) expected (SELECT 4)'));
    });

    it(`26`, () => {
        const frame = RANGE_BETWEEN('', FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`27`, () => {
        const frame = RANGE_BETWEEN(UNBOUNDED_FOLLOWING, 1, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`28`, () => {
        const frame = RANGE_BETWEEN(UNBOUNDED_FOLLOWING, UNBOUNDED_PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`29`, () => {
        const frame = RANGE_BETWEEN(1, UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`30`, () => {
        const frame = RANGE_BETWEEN(1, CURRENT_ROW, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`31`, () => {
        const frame = RANGE_BETWEEN(1, UNBOUNDED_FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`32`, () => {
        const frame = RANGE_BETWEEN(1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`33`, () => {
        const frame = RANGE_BETWEEN(null, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`34`, () => {
        const frame = RANGE_BETWEEN(1, null)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`35`, () => {
        const frame = RANGE_BETWEEN(1, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`36`, () => {
        const frame = RANGE_BETWEEN(CURRENT_ROW, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`37`, () => {
        const frame = RANGE_BETWEEN(1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`38`, () => {
        const frame = RANGE_BETWEEN(PRECEDING, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`39`, () => {
        const frame = RANGE_BETWEEN(1, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`40`, () => {
        const frame = RANGE_BETWEEN(FOLLOWING, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`41`, () => {
        const frame = RANGE_BETWEEN(PRECEDING, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`42`, () => {
        const frame = RANGE_BETWEEN(FOLLOWING, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`43`, () => {
        const frame = RANGE_BETWEEN(FOLLOWING, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`44`, () => {
        const frame = RANGE_BETWEEN(1, PRECEDING, 2)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`45`, () => {
        const frame = RANGE_BETWEEN(1, FOLLOWING, 2, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`46`, () => {
        const frame = RANGE_BETWEEN(1, FOLLOWING, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`47`, () => {
        const frame = RANGE_BETWEEN(FOLLOWING, 1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`48`, () => {
        const frame = RANGE_BETWEEN(FOLLOWING, 1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`49`, () => {
        const frame = RANGE_BETWEEN(PRECEDING, 1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`50`, () => {
        const frame = RANGE_BETWEEN(CURRENT_ROW, 1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`51`, () => {
        const frame = RANGE(null, null)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`52`, () => {
        const frame = RANGE(1, null)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`53`, () => {
        const frame = RANGE(null, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`54`, () => {
        const frame = RANGE(1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`55`, () => {
        const frame = RANGE(FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`56`, () => {
        const frame = RANGE(FOLLOWING, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`57`, () => {
        const frame = RANGE(FOLLOWING, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`58`, () => {
        const frame = RANGE(PRECEDING, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`59`, () => {
        const frame = RANGE(PRECEDING, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`60`, () => {
        const frame = RANGE(PRECEDING, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });
});


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


describe('window functions -> (groups_between + frames):', () => {
    it(`1`, () => {
        const table = `
+----------+------+------+-------+------+-------------------+------+------+
| city     | name | size | count |  sum |               avg |  min |  max |
+----------+------+------+-------+------+-------------------+------+------+
| Berlin   | B    |    1 |     0 | null |              null | null | null |
| Berlin   | C    |    1 |     0 | null |              null | null | null |
| Berlin   | B    |    2 |     2 |    2 |                 1 |    1 |    1 |
| Berlin   | C    |    2 |     2 |    2 |                 1 |    1 |    1 |
| Berlin   | B    |    3 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | C    |    3 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | C    |    3 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | A    |    5 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | B    |    7 |     8 |   20 |               2.5 |    1 |    5 |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    |    3 |     0 | null |              null | null | null |
| Budapest | B    |    5 |     1 |    3 |                 3 |    3 |    3 |
| Budapest | B    |   11 |     2 |    8 |                 4 |    3 |    5 |
| London   | C    | null |     0 | null |              null | null | null |
| London   | C    |    2 |     0 | null |              null | null | null |
| London   | A    |    8 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    8 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    8 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    9 |     4 |   26 |               6.5 |    2 |    8 |
| Madrid   | B    |    2 |     0 | null |              null | null | null |
| Madrid   | B    |    3 |     1 |    2 |                 2 |    2 |    2 |
| Madrid   | B    |   22 |     2 |    5 |               2.5 |    2 |    3 |
| Paris    | A    | null |     0 | null |              null | null | null |
| Paris    | A    |    4 |     0 | null |              null | null | null |
| Paris    | A    |    4 |     0 | null |              null | null | null |
| Paris    | A    |    4 |     0 | null |              null | null | null |
| Paris    | A    |    7 |     3 |   12 |                 4 |    4 |    4 |
| Rome     | A    | null |     0 | null |              null | null | null |
| Rome     | B    | null |     0 | null |              null | null | null |
| Rome     | C    |    2 |     0 | null |              null | null | null |
| Rome     | A    |    4 |     1 |    2 |                 2 |    2 |    2 |
| Rome     | C    |   10 |     2 |    6 |                 3 |    2 |    4 |
| Rome     | C    |   11 |     3 |   16 | 5.333333333333333 |    2 |   10 |
+----------+------+------+-------+------+-------------------+------+------+
36 rows selected`;

        const frame = GROUPS_BETWEEN(UNBOUNDED_PRECEDING, 1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`2`, () => {
        const table = `
+----------+------+------+-------+------+-------------------+------+------+
| city     | name | size | count |  sum |               avg |  min |  max |
+----------+------+------+-------+------+-------------------+------+------+
| Berlin   | B    |    1 |     2 |    2 |                 1 |    1 |    1 |
| Berlin   | C    |    1 |     2 |    2 |                 1 |    1 |    1 |
| Berlin   | B    |    2 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | C    |    2 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | B    |    3 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | C    |    3 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | C    |    3 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | A    |    5 |     8 |   20 |               2.5 |    1 |    5 |
| Berlin   | B    |    7 |     9 |   27 |                 3 |    1 |    7 |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    |    3 |     1 |    3 |                 3 |    3 |    3 |
| Budapest | B    |    5 |     2 |    8 |                 4 |    3 |    5 |
| Budapest | B    |   11 |     3 |   19 | 6.333333333333333 |    3 |   11 |
| London   | C    | null |     0 | null |              null | null | null |
| London   | C    |    2 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    8 |     4 |   26 |               6.5 |    2 |    8 |
| London   | A    |    8 |     4 |   26 |               6.5 |    2 |    8 |
| London   | A    |    8 |     4 |   26 |               6.5 |    2 |    8 |
| London   | A    |    9 |     5 |   35 |                 7 |    2 |    9 |
| Madrid   | B    |    2 |     1 |    2 |                 2 |    2 |    2 |
| Madrid   | B    |    3 |     2 |    5 |               2.5 |    2 |    3 |
| Madrid   | B    |   22 |     3 |   27 |                 9 |    2 |   22 |
| Paris    | A    | null |     0 | null |              null | null | null |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                 4 |    4 |    4 |
| Paris    | A    |    7 |     4 |   19 |              4.75 |    4 |    7 |
| Rome     | A    | null |     0 | null |              null | null | null |
| Rome     | B    | null |     0 | null |              null | null | null |
| Rome     | C    |    2 |     1 |    2 |                 2 |    2 |    2 |
| Rome     | A    |    4 |     2 |    6 |                 3 |    2 |    4 |
| Rome     | C    |   10 |     3 |   16 | 5.333333333333333 |    2 |   10 |
| Rome     | C    |   11 |     4 |   27 |              6.75 |    2 |   11 |
+----------+------+------+-------+------+-------------------+------+------+
36 rows selected`;

        const frame = GROUPS_BETWEEN(UNBOUNDED_PRECEDING, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        const frame2 = GROUPS(UNBOUNDED_PRECEDING)
        const win2 = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame2)
        
        const res2 =
        SELECT('city', 'name', 'size',
            COUNT('size', win2, AS('count')),
            SUM('size', win2, AS('sum')),
            AVG('size', win2, AS('avg')),
            MIN('size', win2, AS('min')),
            MAX('size', win2, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame2 = `GROUPS UNBOUNDED PRECEDING`
        const _win2 = `OVER(PARTITION BY city ORDER BY size ${ _frame2 })`

        const expected2 =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win2 }  AS count,
            SUM(size) ${ _win2 }  AS sum,
            AVG(size) ${ _win2 }  AS avg,
            MIN(size) ${ _win2 }  AS min,
            MAX(size) ${ _win2 }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();


        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
        expect(res).toEqual(res2);
        expect(res2).toEqual(expected2);

    });

    it(`3`, () => {
        const table = `
+----------+------+------+-------+-----+-------------------+-----+-----+
| city     | name | size | count | sum |               avg | min | max |
+----------+------+------+-------+-----+-------------------+-----+-----+
| Berlin   | B    |    1 |     7 |  15 | 2.142857142857143 |   1 |   3 |
| Berlin   | C    |    1 |     7 |  15 | 2.142857142857143 |   1 |   3 |
| Berlin   | B    |    2 |     8 |  20 |               2.5 |   1 |   5 |
| Berlin   | C    |    2 |     8 |  20 |               2.5 |   1 |   5 |
| Berlin   | B    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | A    |    5 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    7 |     9 |  27 |                 3 |   1 |   7 |
| Budapest | B    | null |     2 |   8 |                 4 |   3 |   5 |
| Budapest | B    | null |     2 |   8 |                 4 |   3 |   5 |
| Budapest | C    | null |     2 |   8 |                 4 |   3 |   5 |
| Budapest | C    | null |     2 |   8 |                 4 |   3 |   5 |
| Budapest | C    |    3 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    |    5 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    |   11 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| London   | C    | null |     4 |  26 |               6.5 |   2 |   8 |
| London   | C    |    2 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    9 |     5 |  35 |                 7 |   2 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                 9 |   2 |  22 |
| Madrid   | B    |    3 |     3 |  27 |                 9 |   2 |  22 |
| Madrid   | B    |   22 |     3 |  27 |                 9 |   2 |  22 |
| Paris    | A    | null |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    7 |     4 |  19 |              4.75 |   4 |   7 |
| Rome     | A    | null |     2 |   6 |                 3 |   2 |   4 |
| Rome     | B    | null |     2 |   6 |                 3 |   2 |   4 |
| Rome     | C    |    2 |     3 |  16 | 5.333333333333333 |   2 |  10 |
| Rome     | A    |    4 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |   10 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |   11 |     4 |  27 |              6.75 |   2 |  11 |
+----------+------+------+-------+-----+-------------------+-----+-----+
36 rows selected`;

        const frame = GROUPS_BETWEEN(UNBOUNDED_PRECEDING, 2, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`4`, () => {
        const table = `
+----------+------+------+-------+-----+-------------------+-----+-----+
| city     | name | size | count | sum |               avg | min | max |
+----------+------+------+-------+-----+-------------------+-----+-----+
| Berlin   | B    |    1 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    1 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    2 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    2 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | A    |    5 |     9 |  27 |                 3 |   1 |   7 |
| Berlin   | B    |    7 |     9 |  27 |                 3 |   1 |   7 |
| Budapest | B    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | C    |    3 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    |    5 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    |   11 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| London   | C    | null |     5 |  35 |                 7 |   2 |   9 |
| London   | C    |    2 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    9 |     5 |  35 |                 7 |   2 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                 9 |   2 |  22 |
| Madrid   | B    |    3 |     3 |  27 |                 9 |   2 |  22 |
| Madrid   | B    |   22 |     3 |  27 |                 9 |   2 |  22 |
| Paris    | A    | null |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    7 |     4 |  19 |              4.75 |   4 |   7 |
| Rome     | A    | null |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | B    | null |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |    2 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | A    |    4 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |   10 |     4 |  27 |              6.75 |   2 |  11 |
| Rome     | C    |   11 |     4 |  27 |              6.75 |   2 |  11 |
+----------+------+------+-------+-----+-------------------+-----+-----+
36 rows selected`;

        const frame = GROUPS_BETWEEN(UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`5`, () => {
        const table = `
+----------+------+------+-------+------+-------------------+------+------+
| city     | name | size | count |  sum |               avg |  min |  max |
+----------+------+------+-------+------+-------------------+------+------+
| Berlin   | B    |    1 |     0 | null |              null | null | null |
| Berlin   | C    |    1 |     0 | null |              null | null | null |
| Berlin   | B    |    2 |     2 |    2 |                 1 |    1 |    1 |
| Berlin   | C    |    2 |     2 |    2 |                 1 |    1 |    1 |
| Berlin   | B    |    3 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | C    |    3 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | C    |    3 |     4 |    6 |               1.5 |    1 |    2 |
| Berlin   | A    |    5 |     7 |   15 | 2.142857142857143 |    1 |    3 |
| Berlin   | B    |    7 |     6 |   18 |                 3 |    2 |    5 |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | B    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    | null |     0 | null |              null | null | null |
| Budapest | C    |    3 |     0 | null |              null | null | null |
| Budapest | B    |    5 |     1 |    3 |                 3 |    3 |    3 |
| Budapest | B    |   11 |     2 |    8 |                 4 |    3 |    5 |
| London   | C    | null |     0 | null |              null | null | null |
| London   | C    |    2 |     0 | null |              null | null | null |
| London   | A    |    8 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    8 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    8 |     1 |    2 |                 2 |    2 |    2 |
| London   | A    |    9 |     4 |   26 |               6.5 |    2 |    8 |
| Madrid   | B    |    2 |     0 | null |              null | null | null |
| Madrid   | B    |    3 |     1 |    2 |                 2 |    2 |    2 |
| Madrid   | B    |   22 |     2 |    5 |               2.5 |    2 |    3 |
| Paris    | A    | null |     0 | null |              null | null | null |
| Paris    | A    |    4 |     0 | null |              null | null | null |
| Paris    | A    |    4 |     0 | null |              null | null | null |
| Paris    | A    |    4 |     0 | null |              null | null | null |
| Paris    | A    |    7 |     3 |   12 |                 4 |    4 |    4 |
| Rome     | A    | null |     0 | null |              null | null | null |
| Rome     | B    | null |     0 | null |              null | null | null |
| Rome     | C    |    2 |     0 | null |              null | null | null |
| Rome     | A    |    4 |     1 |    2 |                 2 |    2 |    2 |
| Rome     | C    |   10 |     2 |    6 |                 3 |    2 |    4 |
| Rome     | C    |   11 |     3 |   16 | 5.333333333333333 |    2 |   10 |
+----------+------+------+-------+------+-------------------+------+------+
36 rows selected`;

        const frame = GROUPS_BETWEEN(3, PRECEDING, 1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN 3 PRECEDING AND 1 PRECEDING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`5a`, () => {
        const table = `
+----------+------+------+-------+------+------+------+------+
| city     | name | size | count |  sum |  avg |  min |  max |
+----------+------+------+-------+------+------+------+------+
| Berlin   | B    |    1 |     0 | null | null | null | null |
| Berlin   | C    |    1 |     0 | null | null | null | null |
| Berlin   | B    |    2 |     0 | null | null | null | null |
| Berlin   | C    |    2 |     0 | null | null | null | null |
| Berlin   | B    |    3 |     0 | null | null | null | null |
| Berlin   | C    |    3 |     0 | null | null | null | null |
| Berlin   | C    |    3 |     0 | null | null | null | null |
| Berlin   | A    |    5 |     0 | null | null | null | null |
| Berlin   | B    |    7 |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    |    3 |     0 | null | null | null | null |
| Budapest | B    |    5 |     0 | null | null | null | null |
| Budapest | B    |   11 |     0 | null | null | null | null |
| London   | C    | null |     0 | null | null | null | null |
| London   | C    |    2 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    9 |     0 | null | null | null | null |
| Madrid   | B    |    2 |     0 | null | null | null | null |
| Madrid   | B    |    3 |     0 | null | null | null | null |
| Madrid   | B    |   22 |     0 | null | null | null | null |
| Paris    | A    | null |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    7 |     0 | null | null | null | null |
| Rome     | A    | null |     0 | null | null | null | null |
| Rome     | B    | null |     0 | null | null | null | null |
| Rome     | C    |    2 |     0 | null | null | null | null |
| Rome     | A    |    4 |     0 | null | null | null | null |
| Rome     | C    |   10 |     0 | null | null | null | null |
| Rome     | C    |   11 |     0 | null | null | null | null |
+----------+------+------+-------+------+------+------+------+
36 rows selected`;

        const frame = GROUPS_BETWEEN(1, PRECEDING, 3, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN 1 PRECEDING AND 3 PRECEDING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });


    it(`6`, () => {
        const table = `
+----------+------+------+-------+------+--------------------+------+------+
| city     | name | size | count |  sum |                avg |  min |  max |
+----------+------+------+-------+------+--------------------+------+------+
| Berlin   | B    |    1 |     2 |    2 |                  1 |    1 |    1 |
| Berlin   | C    |    1 |     2 |    2 |                  1 |    1 |    1 |
| Berlin   | B    |    2 |     4 |    6 |                1.5 |    1 |    2 |
| Berlin   | C    |    2 |     4 |    6 |                1.5 |    1 |    2 |
| Berlin   | B    |    3 |     7 |   15 |  2.142857142857143 |    1 |    3 |
| Berlin   | C    |    3 |     7 |   15 |  2.142857142857143 |    1 |    3 |
| Berlin   | C    |    3 |     7 |   15 |  2.142857142857143 |    1 |    3 |
| Berlin   | A    |    5 |     8 |   20 |                2.5 |    1 |    5 |
| Berlin   | B    |    7 |     7 |   25 | 3.5714285714285716 |    2 |    7 |
| Budapest | B    | null |     0 | null |               null | null | null |
| Budapest | B    | null |     0 | null |               null | null | null |
| Budapest | C    | null |     0 | null |               null | null | null |
| Budapest | C    | null |     0 | null |               null | null | null |
| Budapest | C    |    3 |     1 |    3 |                  3 |    3 |    3 |
| Budapest | B    |    5 |     2 |    8 |                  4 |    3 |    5 |
| Budapest | B    |   11 |     3 |   19 |  6.333333333333333 |    3 |   11 |
| London   | C    | null |     0 | null |               null | null | null |
| London   | C    |    2 |     1 |    2 |                  2 |    2 |    2 |
| London   | A    |    8 |     4 |   26 |                6.5 |    2 |    8 |
| London   | A    |    8 |     4 |   26 |                6.5 |    2 |    8 |
| London   | A    |    8 |     4 |   26 |                6.5 |    2 |    8 |
| London   | A    |    9 |     5 |   35 |                  7 |    2 |    9 |
| Madrid   | B    |    2 |     1 |    2 |                  2 |    2 |    2 |
| Madrid   | B    |    3 |     2 |    5 |                2.5 |    2 |    3 |
| Madrid   | B    |   22 |     3 |   27 |                  9 |    2 |   22 |
| Paris    | A    | null |     0 | null |               null | null | null |
| Paris    | A    |    4 |     3 |   12 |                  4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                  4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |                  4 |    4 |    4 |
| Paris    | A    |    7 |     4 |   19 |               4.75 |    4 |    7 |
| Rome     | A    | null |     0 | null |               null | null | null |
| Rome     | B    | null |     0 | null |               null | null | null |
| Rome     | C    |    2 |     1 |    2 |                  2 |    2 |    2 |
| Rome     | A    |    4 |     2 |    6 |                  3 |    2 |    4 |
| Rome     | C    |   10 |     3 |   16 |  5.333333333333333 |    2 |   10 |
| Rome     | C    |   11 |     4 |   27 |               6.75 |    2 |   11 |
+----------+------+------+-------+------+--------------------+------+------+
36 rows selected`;

        const frame = GROUPS_BETWEEN(3, PRECEDING, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN 3 PRECEDING AND CURRENT ROW`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        const frame2 = GROUPS(3, PRECEDING)
        const win2 = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame2)
        
        const res2 =
        SELECT('city', 'name', 'size',
            COUNT('size', win2, AS('count')),
            SUM('size', win2, AS('sum')),
            AVG('size', win2, AS('avg')),
            MIN('size', win2, AS('min')),
            MAX('size', win2, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame2 = `GROUPS 3 PRECEDING`
        const _win2 = `OVER(PARTITION BY city ORDER BY size ${ _frame2 })`

        const expected2 =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win2 }  AS count,
            SUM(size) ${ _win2 }  AS sum,
            AVG(size) ${ _win2 }  AS avg,
            MIN(size) ${ _win2 }  AS min,
            MAX(size) ${ _win2 }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();


        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
        expect(res).toEqual(res2);
        expect(res2).toEqual(expected2);
    });

    it(`7`, () => {
        const table = `
+----------+------+------+-------+-----+--------------------+-----+-----+
| city     | name | size | count | sum |                avg | min | max |
+----------+------+------+-------+-----+--------------------+-----+-----+
| Berlin   | B    |    1 |     7 |  15 |  2.142857142857143 |   1 |   3 |
| Berlin   | C    |    1 |     7 |  15 |  2.142857142857143 |   1 |   3 |
| Berlin   | B    |    2 |     8 |  20 |                2.5 |   1 |   5 |
| Berlin   | C    |    2 |     8 |  20 |                2.5 |   1 |   5 |
| Berlin   | B    |    3 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | A    |    5 |     7 |  25 | 3.5714285714285716 |   2 |   7 |
| Berlin   | B    |    7 |     5 |  21 |                4.2 |   3 |   7 |
| Budapest | B    | null |     2 |   8 |                  4 |   3 |   5 |
| Budapest | B    | null |     2 |   8 |                  4 |   3 |   5 |
| Budapest | C    | null |     2 |   8 |                  4 |   3 |   5 |
| Budapest | C    | null |     2 |   8 |                  4 |   3 |   5 |
| Budapest | C    |    3 |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    |    5 |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    |   11 |     3 |  19 |  6.333333333333333 |   3 |  11 |
| London   | C    | null |     4 |  26 |                6.5 |   2 |   8 |
| London   | C    |    2 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    9 |     5 |  35 |                  7 |   2 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |    3 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |   22 |     3 |  27 |                  9 |   2 |  22 |
| Paris    | A    | null |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    7 |     4 |  19 |               4.75 |   4 |   7 |
| Rome     | A    | null |     2 |   6 |                  3 |   2 |   4 |
| Rome     | B    | null |     2 |   6 |                  3 |   2 |   4 |
| Rome     | C    |    2 |     3 |  16 |  5.333333333333333 |   2 |  10 |
| Rome     | A    |    4 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | C    |   10 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | C    |   11 |     3 |  25 |  8.333333333333334 |   4 |  11 |
+----------+------+------+-------+-----+--------------------+-----+-----+
36 rows selected`;

        const frame = GROUPS_BETWEEN(2, PRECEDING, 2, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN 2 PRECEDING AND 2 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`8`, () => {
        const table = `
+----------+------+------+-------+-----+--------------------+-----+-----+
| city     | name | size | count | sum |                avg | min | max |
+----------+------+------+-------+-----+--------------------+-----+-----+
| Berlin   | B    |    1 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | C    |    1 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | B    |    2 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | C    |    2 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | B    |    3 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | C    |    3 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | A    |    5 |     7 |  25 | 3.5714285714285716 |   2 |   7 |
| Berlin   | B    |    7 |     5 |  21 |                4.2 |   3 |   7 |
| Budapest | B    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    |    3 |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    |    5 |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    |   11 |     3 |  19 |  6.333333333333333 |   3 |  11 |
| London   | C    | null |     5 |  35 |                  7 |   2 |   9 |
| London   | C    |    2 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    8 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    9 |     5 |  35 |                  7 |   2 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |    3 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |   22 |     3 |  27 |                  9 |   2 |  22 |
| Paris    | A    | null |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    7 |     4 |  19 |               4.75 |   4 |   7 |
| Rome     | A    | null |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | B    | null |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | C    |    2 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | A    |    4 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | C    |   10 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | C    |   11 |     3 |  25 |  8.333333333333334 |   4 |  11 |
+----------+------+------+-------+-----+--------------------+-----+-----+
36 rows selected`;

        const frame = GROUPS_BETWEEN(2, PRECEDING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`9`, () => {
        const table = `
+----------+------+------+-------+------+------+------+------+
| city     | name | size | count |  sum |  avg |  min |  max |
+----------+------+------+-------+------+------+------+------+
| Berlin   | B    |    1 |     2 |    2 |    1 |    1 |    1 |
| Berlin   | C    |    1 |     2 |    2 |    1 |    1 |    1 |
| Berlin   | B    |    2 |     2 |    4 |    2 |    2 |    2 |
| Berlin   | C    |    2 |     2 |    4 |    2 |    2 |    2 |
| Berlin   | B    |    3 |     3 |    9 |    3 |    3 |    3 |
| Berlin   | C    |    3 |     3 |    9 |    3 |    3 |    3 |
| Berlin   | C    |    3 |     3 |    9 |    3 |    3 |    3 |
| Berlin   | A    |    5 |     1 |    5 |    5 |    5 |    5 |
| Berlin   | B    |    7 |     1 |    7 |    7 |    7 |    7 |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    |    3 |     1 |    3 |    3 |    3 |    3 |
| Budapest | B    |    5 |     1 |    5 |    5 |    5 |    5 |
| Budapest | B    |   11 |     1 |   11 |   11 |   11 |   11 |
| London   | C    | null |     0 | null | null | null | null |
| London   | C    |    2 |     1 |    2 |    2 |    2 |    2 |
| London   | A    |    8 |     3 |   24 |    8 |    8 |    8 |
| London   | A    |    8 |     3 |   24 |    8 |    8 |    8 |
| London   | A    |    8 |     3 |   24 |    8 |    8 |    8 |
| London   | A    |    9 |     1 |    9 |    9 |    9 |    9 |
| Madrid   | B    |    2 |     1 |    2 |    2 |    2 |    2 |
| Madrid   | B    |    3 |     1 |    3 |    3 |    3 |    3 |
| Madrid   | B    |   22 |     1 |   22 |   22 |   22 |   22 |
| Paris    | A    | null |     0 | null | null | null | null |
| Paris    | A    |    4 |     3 |   12 |    4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |    4 |    4 |    4 |
| Paris    | A    |    4 |     3 |   12 |    4 |    4 |    4 |
| Paris    | A    |    7 |     1 |    7 |    7 |    7 |    7 |
| Rome     | A    | null |     0 | null | null | null | null |
| Rome     | B    | null |     0 | null | null | null | null |
| Rome     | C    |    2 |     1 |    2 |    2 |    2 |    2 |
| Rome     | A    |    4 |     1 |    4 |    4 |    4 |    4 |
| Rome     | C    |   10 |     1 |   10 |   10 |   10 |   10 |
| Rome     | C    |   11 |     1 |   11 |   11 |   11 |   11 |
+----------+------+------+-------+------+------+------+------+
36 rows selected`;

        const frame = GROUPS_BETWEEN(CURRENT_ROW, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN CURRENT ROW AND CURRENT ROW`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`10`, () => {
        const table = `
+----------+------+------+-------+-----+-------------------+-----+-----+
| city     | name | size | count | sum |               avg | min | max |
+----------+------+------+-------+-----+-------------------+-----+-----+
| Berlin   | B    |    1 |     7 |  15 | 2.142857142857143 |   1 |   3 |
| Berlin   | C    |    1 |     7 |  15 | 2.142857142857143 |   1 |   3 |
| Berlin   | B    |    2 |     6 |  18 |                 3 |   2 |   5 |
| Berlin   | C    |    2 |     6 |  18 |                 3 |   2 |   5 |
| Berlin   | B    |    3 |     5 |  21 |               4.2 |   3 |   7 |
| Berlin   | C    |    3 |     5 |  21 |               4.2 |   3 |   7 |
| Berlin   | C    |    3 |     5 |  21 |               4.2 |   3 |   7 |
| Berlin   | A    |    5 |     2 |  12 |                 6 |   5 |   7 |
| Berlin   | B    |    7 |     1 |   7 |                 7 |   7 |   7 |
| Budapest | B    | null |     2 |   8 |                 4 |   3 |   5 |
| Budapest | B    | null |     2 |   8 |                 4 |   3 |   5 |
| Budapest | C    | null |     2 |   8 |                 4 |   3 |   5 |
| Budapest | C    | null |     2 |   8 |                 4 |   3 |   5 |
| Budapest | C    |    3 |     3 |  19 | 6.333333333333333 |   3 |  11 |
| Budapest | B    |    5 |     2 |  16 |                 8 |   5 |  11 |
| Budapest | B    |   11 |     1 |  11 |                11 |  11 |  11 |
| London   | C    | null |     4 |  26 |               6.5 |   2 |   8 |
| London   | C    |    2 |     5 |  35 |                 7 |   2 |   9 |
| London   | A    |    8 |     4 |  33 |              8.25 |   8 |   9 |
| London   | A    |    8 |     4 |  33 |              8.25 |   8 |   9 |
| London   | A    |    8 |     4 |  33 |              8.25 |   8 |   9 |
| London   | A    |    9 |     1 |   9 |                 9 |   9 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                 9 |   2 |  22 |
| Madrid   | B    |    3 |     2 |  25 |              12.5 |   3 |  22 |
| Madrid   | B    |   22 |     1 |  22 |                22 |  22 |  22 |
| Paris    | A    | null |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |              4.75 |   4 |   7 |
| Paris    | A    |    7 |     1 |   7 |                 7 |   7 |   7 |
| Rome     | A    | null |     2 |   6 |                 3 |   2 |   4 |
| Rome     | B    | null |     2 |   6 |                 3 |   2 |   4 |
| Rome     | C    |    2 |     3 |  16 | 5.333333333333333 |   2 |  10 |
| Rome     | A    |    4 |     3 |  25 | 8.333333333333334 |   4 |  11 |
| Rome     | C    |   10 |     2 |  21 |              10.5 |  10 |  11 |
| Rome     | C    |   11 |     1 |  11 |                11 |  11 |  11 |
+----------+------+------+-------+-----+-------------------+-----+-----+
36 rows selected`;

        const frame = GROUPS_BETWEEN(CURRENT_ROW, 2, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN CURRENT ROW AND 2 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        const frame2 = GROUPS(2, FOLLOWING)
        const win2 = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame2)
        
        const res2 =
        SELECT('city', 'name', 'size',
            COUNT('size', win2, AS('count')),
            SUM('size', win2, AS('sum')),
            AVG('size', win2, AS('avg')),
            MIN('size', win2, AS('min')),
            MAX('size', win2, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
        expect(res).toEqual(res2);
    });

    it(`11`, () => {
        const table = `
+----------+------+------+-------+-----+--------------------+-----+-----+
| city     | name | size | count | sum |                avg | min | max |
+----------+------+------+-------+-----+--------------------+-----+-----+
| Berlin   | B    |    1 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | C    |    1 |     9 |  27 |                  3 |   1 |   7 |
| Berlin   | B    |    2 |     7 |  25 | 3.5714285714285716 |   2 |   7 |
| Berlin   | C    |    2 |     7 |  25 | 3.5714285714285716 |   2 |   7 |
| Berlin   | B    |    3 |     5 |  21 |                4.2 |   3 |   7 |
| Berlin   | C    |    3 |     5 |  21 |                4.2 |   3 |   7 |
| Berlin   | C    |    3 |     5 |  21 |                4.2 |   3 |   7 |
| Berlin   | A    |    5 |     2 |  12 |                  6 |   5 |   7 |
| Berlin   | B    |    7 |     1 |   7 |                  7 |   7 |   7 |
| Budapest | B    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    | null |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | C    |    3 |     3 |  19 |  6.333333333333333 |   3 |  11 |
| Budapest | B    |    5 |     2 |  16 |                  8 |   5 |  11 |
| Budapest | B    |   11 |     1 |  11 |                 11 |  11 |  11 |
| London   | C    | null |     5 |  35 |                  7 |   2 |   9 |
| London   | C    |    2 |     5 |  35 |                  7 |   2 |   9 |
| London   | A    |    8 |     4 |  33 |               8.25 |   8 |   9 |
| London   | A    |    8 |     4 |  33 |               8.25 |   8 |   9 |
| London   | A    |    8 |     4 |  33 |               8.25 |   8 |   9 |
| London   | A    |    9 |     1 |   9 |                  9 |   9 |   9 |
| Madrid   | B    |    2 |     3 |  27 |                  9 |   2 |  22 |
| Madrid   | B    |    3 |     2 |  25 |               12.5 |   3 |  22 |
| Madrid   | B    |   22 |     1 |  22 |                 22 |  22 |  22 |
| Paris    | A    | null |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    4 |     4 |  19 |               4.75 |   4 |   7 |
| Paris    | A    |    7 |     1 |   7 |                  7 |   7 |   7 |
| Rome     | A    | null |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | B    | null |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | C    |    2 |     4 |  27 |               6.75 |   2 |  11 |
| Rome     | A    |    4 |     3 |  25 |  8.333333333333334 |   4 |  11 |
| Rome     | C    |   10 |     2 |  21 |               10.5 |  10 |  11 |
| Rome     | C    |   11 |     1 |  11 |                 11 |  11 |  11 |
+----------+------+------+-------+-----+--------------------+-----+-----+
36 rows selected`;

        const frame = GROUPS_BETWEEN(CURRENT_ROW, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        const frame2 = GROUPS(UNBOUNDED_FOLLOWING)
        const win2 = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame2)
        
        const res2 =
        SELECT('city', 'name', 'size',
            COUNT('size', win2, AS('count')),
            SUM('size', win2, AS('sum')),
            AVG('size', win2, AS('avg')),
            MIN('size', win2, AS('min')),
            MAX('size', win2, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
        expect(res).toEqual(res2);
    });

    it(`12`, () => {
        const table = `
+----------+------+------+-------+------+-------------------+------+------+
| city     | name | size | count |  sum |               avg |  min |  max |
+----------+------+------+-------+------+-------------------+------+------+
| Berlin   | B    |    1 |     6 |   18 |                 3 |    2 |    5 |
| Berlin   | C    |    1 |     6 |   18 |                 3 |    2 |    5 |
| Berlin   | B    |    2 |     5 |   21 |               4.2 |    3 |    7 |
| Berlin   | C    |    2 |     5 |   21 |               4.2 |    3 |    7 |
| Berlin   | B    |    3 |     2 |   12 |                 6 |    5 |    7 |
| Berlin   | C    |    3 |     2 |   12 |                 6 |    5 |    7 |
| Berlin   | C    |    3 |     2 |   12 |                 6 |    5 |    7 |
| Berlin   | A    |    5 |     1 |    7 |                 7 |    7 |    7 |
| Berlin   | B    |    7 |     0 | null |              null | null | null |
| Budapest | B    | null |     3 |   19 | 6.333333333333333 |    3 |   11 |
| Budapest | B    | null |     3 |   19 | 6.333333333333333 |    3 |   11 |
| Budapest | C    | null |     3 |   19 | 6.333333333333333 |    3 |   11 |
| Budapest | C    | null |     3 |   19 | 6.333333333333333 |    3 |   11 |
| Budapest | C    |    3 |     2 |   16 |                 8 |    5 |   11 |
| Budapest | B    |    5 |     1 |   11 |                11 |   11 |   11 |
| Budapest | B    |   11 |     0 | null |              null | null | null |
| London   | C    | null |     5 |   35 |                 7 |    2 |    9 |
| London   | C    |    2 |     4 |   33 |              8.25 |    8 |    9 |
| London   | A    |    8 |     1 |    9 |                 9 |    9 |    9 |
| London   | A    |    8 |     1 |    9 |                 9 |    9 |    9 |
| London   | A    |    8 |     1 |    9 |                 9 |    9 |    9 |
| London   | A    |    9 |     0 | null |              null | null | null |
| Madrid   | B    |    2 |     2 |   25 |              12.5 |    3 |   22 |
| Madrid   | B    |    3 |     1 |   22 |                22 |   22 |   22 |
| Madrid   | B    |   22 |     0 | null |              null | null | null |
| Paris    | A    | null |     4 |   19 |              4.75 |    4 |    7 |
| Paris    | A    |    4 |     1 |    7 |                 7 |    7 |    7 |
| Paris    | A    |    4 |     1 |    7 |                 7 |    7 |    7 |
| Paris    | A    |    4 |     1 |    7 |                 7 |    7 |    7 |
| Paris    | A    |    7 |     0 | null |              null | null | null |
| Rome     | A    | null |     3 |   16 | 5.333333333333333 |    2 |   10 |
| Rome     | B    | null |     3 |   16 | 5.333333333333333 |    2 |   10 |
| Rome     | C    |    2 |     3 |   25 | 8.333333333333334 |    4 |   11 |
| Rome     | A    |    4 |     2 |   21 |              10.5 |   10 |   11 |
| Rome     | C    |   10 |     1 |   11 |                11 |   11 |   11 |
| Rome     | C    |   11 |     0 | null |              null | null | null |
+----------+------+------+-------+------+-------------------+------+------+
36 rows selected`;

        const frame = GROUPS_BETWEEN(1, FOLLOWING, 3, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN 1 FOLLOWING AND 3 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`12a`, () => {
        const table = `
+----------+------+------+-------+------+------+------+------+
| city     | name | size | count |  sum |  avg |  min |  max |
+----------+------+------+-------+------+------+------+------+
| Berlin   | B    |    1 |     0 | null | null | null | null |
| Berlin   | C    |    1 |     0 | null | null | null | null |
| Berlin   | B    |    2 |     0 | null | null | null | null |
| Berlin   | C    |    2 |     0 | null | null | null | null |
| Berlin   | B    |    3 |     0 | null | null | null | null |
| Berlin   | C    |    3 |     0 | null | null | null | null |
| Berlin   | C    |    3 |     0 | null | null | null | null |
| Berlin   | A    |    5 |     0 | null | null | null | null |
| Berlin   | B    |    7 |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | B    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    | null |     0 | null | null | null | null |
| Budapest | C    |    3 |     0 | null | null | null | null |
| Budapest | B    |    5 |     0 | null | null | null | null |
| Budapest | B    |   11 |     0 | null | null | null | null |
| London   | C    | null |     0 | null | null | null | null |
| London   | C    |    2 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    8 |     0 | null | null | null | null |
| London   | A    |    9 |     0 | null | null | null | null |
| Madrid   | B    |    2 |     0 | null | null | null | null |
| Madrid   | B    |    3 |     0 | null | null | null | null |
| Madrid   | B    |   22 |     0 | null | null | null | null |
| Paris    | A    | null |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    4 |     0 | null | null | null | null |
| Paris    | A    |    7 |     0 | null | null | null | null |
| Rome     | A    | null |     0 | null | null | null | null |
| Rome     | B    | null |     0 | null | null | null | null |
| Rome     | C    |    2 |     0 | null | null | null | null |
| Rome     | A    |    4 |     0 | null | null | null | null |
| Rome     | C    |   10 |     0 | null | null | null | null |
| Rome     | C    |   11 |     0 | null | null | null | null |
+----------+------+------+-------+------+------+------+------+
36 rows selected`;

        const frame = GROUPS_BETWEEN(3, FOLLOWING, 1, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN 3 FOLLOWING AND 1 FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });


    it(`13`, () => {
        const table = `
+----------+------+------+-------+------+--------------------+------+------+
| city     | name | size | count |  sum |                avg |  min |  max |
+----------+------+------+-------+------+--------------------+------+------+
| Berlin   | B    |    1 |     7 |   25 | 3.5714285714285716 |    2 |    7 |
| Berlin   | C    |    1 |     7 |   25 | 3.5714285714285716 |    2 |    7 |
| Berlin   | B    |    2 |     5 |   21 |                4.2 |    3 |    7 |
| Berlin   | C    |    2 |     5 |   21 |                4.2 |    3 |    7 |
| Berlin   | B    |    3 |     2 |   12 |                  6 |    5 |    7 |
| Berlin   | C    |    3 |     2 |   12 |                  6 |    5 |    7 |
| Berlin   | C    |    3 |     2 |   12 |                  6 |    5 |    7 |
| Berlin   | A    |    5 |     1 |    7 |                  7 |    7 |    7 |
| Berlin   | B    |    7 |     0 | null |               null | null | null |
| Budapest | B    | null |     3 |   19 |  6.333333333333333 |    3 |   11 |
| Budapest | B    | null |     3 |   19 |  6.333333333333333 |    3 |   11 |
| Budapest | C    | null |     3 |   19 |  6.333333333333333 |    3 |   11 |
| Budapest | C    | null |     3 |   19 |  6.333333333333333 |    3 |   11 |
| Budapest | C    |    3 |     2 |   16 |                  8 |    5 |   11 |
| Budapest | B    |    5 |     1 |   11 |                 11 |   11 |   11 |
| Budapest | B    |   11 |     0 | null |               null | null | null |
| London   | C    | null |     5 |   35 |                  7 |    2 |    9 |
| London   | C    |    2 |     4 |   33 |               8.25 |    8 |    9 |
| London   | A    |    8 |     1 |    9 |                  9 |    9 |    9 |
| London   | A    |    8 |     1 |    9 |                  9 |    9 |    9 |
| London   | A    |    8 |     1 |    9 |                  9 |    9 |    9 |
| London   | A    |    9 |     0 | null |               null | null | null |
| Madrid   | B    |    2 |     2 |   25 |               12.5 |    3 |   22 |
| Madrid   | B    |    3 |     1 |   22 |                 22 |   22 |   22 |
| Madrid   | B    |   22 |     0 | null |               null | null | null |
| Paris    | A    | null |     4 |   19 |               4.75 |    4 |    7 |
| Paris    | A    |    4 |     1 |    7 |                  7 |    7 |    7 |
| Paris    | A    |    4 |     1 |    7 |                  7 |    7 |    7 |
| Paris    | A    |    4 |     1 |    7 |                  7 |    7 |    7 |
| Paris    | A    |    7 |     0 | null |               null | null | null |
| Rome     | A    | null |     4 |   27 |               6.75 |    2 |   11 |
| Rome     | B    | null |     4 |   27 |               6.75 |    2 |   11 |
| Rome     | C    |    2 |     3 |   25 |  8.333333333333334 |    4 |   11 |
| Rome     | A    |    4 |     2 |   21 |               10.5 |   10 |   11 |
| Rome     | C    |   10 |     1 |   11 |                 11 |   11 |   11 |
| Rome     | C    |   11 |     0 | null |               null | null | null |
+----------+------+------+-------+------+--------------------+------+------+
36 rows selected`;

        const frame = GROUPS_BETWEEN(1, FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', win, AS('count')),
            SUM('size', win, AS('sum')),
            AVG('size', win, AS('avg')),
            MIN('size', win, AS('min')),
            MAX('size', win, AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        const _frame = `GROUPS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING`
        const _win = `OVER(PARTITION BY city ORDER BY size ${ _frame })`

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) ${ _win }  AS count,
            SUM(size) ${ _win }  AS sum,
            AVG(size) ${ _win }  AS avg,
            MIN(size) ${ _win }  AS min,
            MAX(size) ${ _win }  AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`14`, () => {
        const frame = GROUPS_BETWEEN(1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('more frame boundaries expected (SELECT 4)'));
    });

    it(`15`, () => {
        const frame = GROUPS_BETWEEN(1, 1, 1, 1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('less frame boundaries expected (SELECT 4)'));
    });

    it(`16`, () => {
        const frame = GROUPS_BETWEEN(UNBOUNDED_PRECEDING, UNBOUNDED_PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`17`, () => {
        const frame = GROUPS_BETWEEN(UNBOUNDED_PRECEDING, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });


    it(`18`, () => {
        const frame = GROUPS_BETWEEN(UNBOUNDED_PRECEDING, -1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('integer (>= 0) expected (SELECT 4)'));
    });

    it(`19`, () => {
        const frame = GROUPS_BETWEEN(UNBOUNDED_PRECEDING, 1.2, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('integer (>= 0) expected (SELECT 4)'));
    });

    it(`20`, () => {
        const frame = GROUPS_BETWEEN(UNBOUNDED_PRECEDING, 1, UNBOUNDED_PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`21`, () => {
        const frame = GROUPS_BETWEEN(UNBOUNDED_PRECEDING, 1, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`22`, () => {
        const frame = GROUPS_BETWEEN(UNBOUNDED_PRECEDING, 1, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`23`, () => {
        const frame = GROUPS_BETWEEN(UNBOUNDED_FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`24`, () => {
        const frame = GROUPS_BETWEEN(1, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`25`, () => {
        const frame = GROUPS_BETWEEN(-1, FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('integer (>= 0) expected (SELECT 4)'));
    });

    it(`26`, () => {
        const frame = GROUPS_BETWEEN('', FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`27`, () => {
        const frame = GROUPS_BETWEEN(UNBOUNDED_FOLLOWING, 1, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`28`, () => {
        const frame = GROUPS_BETWEEN(UNBOUNDED_FOLLOWING, UNBOUNDED_PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`29`, () => {
        const frame = GROUPS_BETWEEN(1, UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`30`, () => {
        const frame = GROUPS_BETWEEN(1, CURRENT_ROW, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`31`, () => {
        const frame = GROUPS_BETWEEN(1, UNBOUNDED_FOLLOWING, UNBOUNDED_FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`32`, () => {
        const frame = GROUPS_BETWEEN(1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`33`, () => {
        const frame = GROUPS_BETWEEN(null, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`34`, () => {
        const frame = GROUPS_BETWEEN(1, null)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`35`, () => {
        const frame = GROUPS_BETWEEN(1, CURRENT_ROW)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`36`, () => {
        const frame = GROUPS_BETWEEN(CURRENT_ROW, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`37`, () => {
        const frame = GROUPS_BETWEEN(1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`38`, () => {
        const frame = GROUPS_BETWEEN(PRECEDING, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`39`, () => {
        const frame = GROUPS_BETWEEN(1, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`40`, () => {
        const frame = GROUPS_BETWEEN(FOLLOWING, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`41`, () => {
        const frame = GROUPS_BETWEEN(PRECEDING, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`42`, () => {
        const frame = GROUPS_BETWEEN(FOLLOWING, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`43`, () => {
        const frame = GROUPS_BETWEEN(FOLLOWING, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`44`, () => {
        const frame = GROUPS_BETWEEN(1, PRECEDING, 2)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`45`, () => {
        const frame = GROUPS_BETWEEN(1, FOLLOWING, 2, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`46`, () => {
        const frame = GROUPS_BETWEEN(1, FOLLOWING, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`47`, () => {
        const frame = GROUPS_BETWEEN(FOLLOWING, 1, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`48`, () => {
        const frame = GROUPS_BETWEEN(FOLLOWING, 1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`49`, () => {
        const frame = GROUPS_BETWEEN(PRECEDING, 1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`50`, () => {
        const frame = GROUPS_BETWEEN(CURRENT_ROW, 1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`51`, () => {
        const frame = GROUPS(null, null)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`52`, () => {
        const frame = GROUPS(1, null)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`53`, () => {
        const frame = GROUPS(null, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`54`, () => {
        const frame = GROUPS(1, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`55`, () => {
        const frame = GROUPS(FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`56`, () => {
        const frame = GROUPS(FOLLOWING, 1)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`57`, () => {
        const frame = GROUPS(FOLLOWING, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`58`, () => {
        const frame = GROUPS(PRECEDING, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`59`, () => {
        const frame = GROUPS(PRECEDING, PRECEDING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });

    it(`60`, () => {
        const frame = GROUPS(PRECEDING, FOLLOWING)
        const win = OVER(PARTITION_BY('city'), ORDER_BY('size'), frame)
        
        const query = () =>
        SELECT('city', 'name', 'size',
            SUM('size', win, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')))
    
        expect(query).toThrow(new Error('invalid frame boundaries (SELECT 4)'));
    });
});
