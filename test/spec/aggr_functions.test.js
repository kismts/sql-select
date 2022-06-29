import { SELECT, FROM, JOIN, ON, ORDER_BY, ADD, AS, WHERE,
    SUB, COUNT, SUM, AVG, MIN, MAX, GROUP_BY, COUNT_DISTINCT, SUM_DISTINCT, AVG_DISTINCT, HAVING } from '../../dist/select.js'
import { input4, input5, createTable } from './data.js'
import { drawTable } from './utils.js'
const trim = str => str.trim();

import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const sqlite = require('better-sqlite3');
const db = sqlite(':memory:');


createTable(db, input4, 'input4')
createTable(db, input5, 'input5')



describe('aggregate functions (without groupby):', () => {
    it(`aggregate functions 1`, () => {
        const table = `
+-------+-----------+-----+-------------------+-----+-----+
| count | count_all | sum |               avg | min | max |
+-------+-----------+-----+-------------------+-----+-----+
|     9 |        12 |  37 | 4.111111111111111 |   1 |  11 |
+-------+-----------+-----+-------------------+-----+-----+
1 row selected`;
        
        const res =
        SELECT(
            COUNT('size', AS('count')),
            COUNT('*', AS('count_all')),
            SUM('size', AS('sum')),
            AVG('size', AS('avg')),
            MIN('size', AS('min')),
            MAX('size', AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        const expected =
        db.prepare(`
        SELECT
            COUNT(size) AS count,
            COUNT(*) AS count_all,
            SUM(size) AS sum,
            AVG(size) AS avg,
            MIN(size) AS min,
            MAX(size) AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`distinct aggregate functions 1`, () => {
        const table = `
+-------+-----+-----+
| count | sum | avg |
+-------+-----+-----+
|     5 |  27 | 5.4 |
+-------+-----+-----+
1 row selected`;
        
        const res =
        SELECT(
            COUNT_DISTINCT('size', AS('count')),
            SUM_DISTINCT('size', AS('sum')),
            AVG_DISTINCT('size', AS('avg')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        const expected =
        db.prepare(`
        SELECT
            COUNT(DISTINCT size) AS count,
            SUM(DISTINCT size) AS sum,
            AVG(DISTINCT size) AS avg
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });



    it(`aggregate functions nesting 1`, () => {
        const table = `
+-----+
| sum |
+-----+
|  73 |
+-----+
1 row selected`;
        
        const res =
        SELECT(
            SUM(ADD('t1.id', SUB('t2.age', 'type'), 'size'), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        const expected =
        db.prepare(`
        SELECT
            SUM(t1.id + (t2.age - type) + size) AS sum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`aggregate functions nesting 2`, () => {
        const table = `
+-----+
| sum |
+-----+
|  51 |
+-----+
1 row selected`;
        
        const res =
        SELECT(
            SUM_DISTINCT(ADD('t1.id', SUB('t2.age', 'type'), 'size'), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        const expected =
        db.prepare(`
        SELECT
            SUM(DISTINCT t1.id + (t2.age - type) + size) AS sum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });


    it(`aggregate functions nesting 3`, () => {
        const table = `
+------+------+------+
| sum1 | sum2 | sum3 |
+------+------+------+
|   37 |   48 |   85 |
+------+------+------+
1 row selected`;
        
        const res =
        SELECT(
            SUM('size', AS('sum1')),
            SUM('type', AS('sum2')),
            ADD('sum1', 'sum2', AS('sum3')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum1'))

        const expected =
        db.prepare(`
        SELECT
            SUM(size) AS sum1,
            SUM(type) AS sum2,
            SUM(size) + SUM(type) AS sum3
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum1`).all();

        expect(trim(drawTable(res))).toEqual(trim(table)); 
        expect(res).toEqual(expected);                         
    });

    it(`aggregate functions nesting 4`, () => {
        const table = `
+------+------+------+------+
| sum1 | sum2 | sum3 | sum4 |
+------+------+------+------+
|   37 |   48 |   85 |  122 |
+------+------+------+------+
1 row selected`;
        
        const res =
        SELECT(
            SUM('size', AS('sum1')),
            SUM('type', AS('sum2')),
            ADD('sum1', 'sum2', AS('sum3')),
            ADD('sum3', 'sum1', AS('sum4')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum1'))

        const expected =
        db.prepare(`
        SELECT
            SUM(size) AS sum1,
            SUM(type) AS sum2,
            SUM(size) + SUM(type) AS sum3,
            SUM(size) + SUM(type) + SUM(size) AS sum4
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum1`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));  
        expect(res).toEqual(expected);                                                
    });

    it(`aggregate functions nesting 5`, () => {
        const table = `
+-----+
| sum |
+-----+
|  85 |
+-----+
1 row selected`;
        
        const res =
        SELECT(
            ADD(SUM('size'), SUM('type'), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        const expected =
        db.prepare(`
        SELECT
            SUM(size) + SUM(type) AS sum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum`).all();


        expect(trim(drawTable(res))).toEqual(trim(table));          
        expect(res).toEqual(expected);
    });

    it(`aggregate functions nesting 5`, () => {
        const table = `
+------+------+
| sum1 | sum2 |
+------+------+
|   37 |   85 |
+------+------+
1 row selected`;
        
        const res =
        SELECT(
            SUM('size', AS('sum1')),
            ADD('sum1', SUM('type'), AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum1'))

        const expected =
        db.prepare(`
        SELECT
            SUM(size) AS sum1,
            SUM(size) + SUM(type) AS sum2
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum1`).all();


        expect(trim(drawTable(res))).toEqual(trim(table));          
        expect(res).toEqual(expected);
    });




    it(`1`, () => {
        const query = () =>
        SELECT(
            COUNT('sizexxx', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('column sizexxx (SELECT 1) does not exist'));
    });

    it(`2`, () => {
        const query = () =>
        SELECT(
            COUNT('id', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('column id (SELECT 1) ambiguous'));
    });


    it(`3`, () => {
        const query = () =>
        SELECT(
            COUNT('size', AS('count')),
            SUM('size'),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('AS expected (SELECT 2)'));
    });

    it(`4`, () => {
        const query = () =>
        SELECT(
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('t1.id'))

        expect(query).toThrow(new Error('column t1.id (ORDER_BY) does not exist'));
    });

    it(`5`, () => {
        const query = () =>
        SELECT(
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('id'))

        expect(query).toThrow(new Error('column id (ORDER_BY) does not exist'));
    });


    it(`6`, () => {
        const query = () =>
        SELECT(
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.sum > 3),
        ORDER_BY('t1.id'))

        expect(query).toThrow(new Error('column sum (WHERE) does not exist'));
    });

    it(`6`, () => {
        const query = () =>
        SELECT(
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        HAVING(r => r.sum > 3),
        ORDER_BY('t1.id'))

        expect(query).toThrow(new Error('GROUP_BY required [before HAVING]'));
    });


    it(`7`, () => {
        const query = () =>
        SELECT(
            COUNT('size', AS('count')),
            SUM(ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('nested AS (SELECT 2)'));
    });

    it(`nesting`, () => {
        const query = () =>
        SELECT(
            COUNT('size', AS('count')),
            SUM(ADD('t1.id', SUB('t2.age', 'typexxx'), 'size'), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('column typexxx (SELECT 2) does not exist'));
    });

    it(`6`, () => {
        const query = () =>
        SELECT(
            SUM('', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('invalid parameter (SELECT 1)'));
    });

    it(`6`, () => {
        const query = () =>
        SELECT(
            SUM({}, AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('invalid parameter (SELECT 1)'));
    });

    it(`7`, () => {
        const query = () =>
        SELECT(
            SUM(SUM('size'), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('nesting error (SELECT 1)'));
    });

    it(`8`, () => {
        const query = () =>
        SELECT(
            SUM(ADD('t1.id', SUB(SUM('size'), 'type'), 'size'), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('nesting error (SELECT 1)'));
    });

    it(`9`, () => {
        const query = () =>
        SELECT(
            SUM('size', AS('sum1')),
            SUM('sum1', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('nesting error (SELECT 2 sum1)'));
    });

    it(`10`, () => {        
        const query = () =>
        SELECT(
            SUM('size', AS('sum1')),
            SUM('type', AS('sum2')),
            ADD('sum3', 'sum1', AS('sum4')),
            ADD('sum1', 'sum2', AS('sum3')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum1'))

        expect(query).toThrow(new Error('column sum3 (SELECT 3) does not exist'));
    });

    it(`11`, () => {        
        const query = () =>
        SELECT(
            SUM('size', AS('sum1')),
            ADD('size', 'sum1', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum1'))

        expect(query).toThrow(new Error('column size (SELECT 2) does not exist'));
    });

    it(`11`, () => {        
        const query = () =>
        SELECT(
            ADD('size', SUM('size'), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('column size (SELECT 1) does not exist'));
    });
});




describe('groupby (without aggregate functions):', () => {
    it(`groupby 1`, () => {
        const table = `
+----------+------+------+
| city     | name | size |
+----------+------+------+
| Berlin   | A    |    5 |
| Berlin   | B    |    7 |
| Budapest | B    |    5 |
| Budapest | B    |   11 |
| London   | A    |    8 |
| London   | A    |    9 |
+----------+------+------+
6 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.size > 3),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name, size
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        HAVING size > 3
        ORDER BY city, name`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`groupby 2`, () => {
        const table = `
+----------+------+
| city     | name |
+----------+------+
| Berlin   | A    |
| Berlin   | B    |
| Berlin   | B    |
| Berlin   | B    |
| Berlin   | B    |
| Berlin   | C    |
| Berlin   | C    |
| Berlin   | C    |
| Budapest | B    |
| Budapest | B    |
| Budapest | C    |
+----------+------+
11 rows selected`;
        
        const res =
        SELECT('city', 'name',
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'L'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.size > 0),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'L'
        GROUP BY city, name, size
        HAVING size > 0
        ORDER BY city, name`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });


    it(`groupby with scalar function`, () => {
        const table = `
+--------+-----+
| city   | add |
+--------+-----+
| Berlin |  20 |
| London |  18 |
| London |  23 |
| Madrid |  32 |
+--------+-----+
4 rows selected`;
        
        const res =
        SELECT('city',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.size > 4),
        GROUP_BY('city', 'add'),
        HAVING(r => r.add > 11),
        ORDER_BY('city', 'add'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });


    it(`groupby with scalar function`, () => {
        const table = `
+----------+-----+
| city     | add |
+----------+-----+
| Berlin   |   8 |
| Berlin   |   9 |
| Berlin   |  10 |
| Berlin   |  11 |
| Berlin   |  17 |
| Berlin   |  20 |
| Budapest |   6 |
| London   |  10 |
| London   |  18 |
| London   |  23 |
+----------+-----+
10 rows selected`;
        
        const res =
        SELECT('city',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'add'),
        HAVING(r => r.add > 3),
        ORDER_BY('city', 'add'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`groupby with scalar function`, () => {
        const table = `
+----------+-----+
| city     | add |
+----------+-----+
| Berlin   |   8 |
| Berlin   |   9 |
| Berlin   |  10 |
| Berlin   |  11 |
| Berlin   |  17 |
| Berlin   |  20 |
| Budapest |   6 |
| London   |  10 |
| London   |  18 |
| London   |  23 |
+----------+-----+
10 rows selected`;
        
        const res =
        SELECT('city',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 0 && r.city < 'M'),
        GROUP_BY('city', 'add'),
        HAVING(r => r.add > 3),
        ORDER_BY('city', 'add'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });




    it(`1`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column size (SELECT 3) does not exist in GROUP_BY'));
    });

    it(`2`, () => {
        const query = () =>
        SELECT('city', 'name',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column add (SELECT 3) does not exist in GROUP_BY'));
    });

    it(`3`, () => {
        const query = () =>
        SELECT('city', 'name', 
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city', 'name'),
        HAVING(r => r.size > 0),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column size (HAVING) does not exist'));
    });

    it(`4`, () => {
        const query = () =>
        SELECT('city', 'name', 
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city', 'name'),
        HAVING(r => r.city > 'M'),
        ORDER_BY('city', 'size'))

        expect(query).toThrow(new Error('column size (ORDER_BY) does not exist'));
    });


    it(`3`, () => {
        const query = () =>
        SELECT('city', 'name',
            ADD('t1.id', SUB('t2.age', 'typexxx'), 'size', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'add'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column typexxx (SELECT 3) does not exist'));
    });


    it(`4`, () => {
        const query = () =>
        SELECT('city', 'name',
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name'),
        HAVING(r => r.id > 0),
        ORDER_BY('city', 'id'))

        expect(query).toThrow(new Error('column id (HAVING) does not exist'));
    });

    it(`5`, () => {
        const query = () =>
        SELECT('city', 'name',
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name'),
        ORDER_BY('city', 'id'))

        expect(query).toThrow(new Error('column id (ORDER_BY) does not exist'));
    });

    it(`6`, () => {
        const query = () =>
        SELECT('city', 'name',
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'namexxx'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column namexxx (GROUP_BY) does not exist'));
    });

    it(`7`, () => {
        const query = () =>
        SELECT('*',
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size', 't1.id', 't1.age'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column type (SELECT 4) does not exist in GROUP_BY'));
    });

});


describe('groupby with aggregate functions:', () => {
    it(`groupby 1`, () => {
        const table = `
+----------+------+------+-------+-----------+------+------+------+------+
| city     | name | size | count | count_all |  sum |  avg |  min |  max |
+----------+------+------+-------+-----------+------+------+------+------+
| Berlin   | A    |    5 |     1 |         1 |    5 |    5 |    5 |    5 |
| Berlin   | B    |    1 |     1 |         1 |    1 |    1 |    1 |    1 |
| Berlin   | B    |    2 |     1 |         1 |    2 |    2 |    2 |    2 |
| Berlin   | B    |    3 |     1 |         1 |    3 |    3 |    3 |    3 |
| Berlin   | B    |    7 |     1 |         1 |    7 |    7 |    7 |    7 |
| Berlin   | C    |    1 |     1 |         1 |    1 |    1 |    1 |    1 |
| Berlin   | C    |    2 |     1 |         1 |    2 |    2 |    2 |    2 |
| Berlin   | C    |    3 |     2 |         2 |    6 |    3 |    3 |    3 |
| Budapest | B    | null |     0 |         2 | null | null | null | null |
| Budapest | B    |    5 |     1 |         1 |    5 |    5 |    5 |    5 |
| Budapest | B    |   11 |     1 |         1 |   11 |   11 |   11 |   11 |
| Budapest | C    | null |     0 |         2 | null | null | null | null |
| Budapest | C    |    3 |     1 |         1 |    3 |    3 |    3 |    3 |
| London   | A    |    8 |     3 |         3 |   24 |    8 |    8 |    8 |
| London   | A    |    9 |     1 |         1 |    9 |    9 |    9 |    9 |
| London   | C    | null |     0 |         1 | null | null | null | null |
| London   | C    |    2 |     1 |         1 |    2 |    2 |    2 |    2 |
+----------+------+------+-------+-----------+------+------+------+------+
17 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', AS('count')),
            COUNT('*', AS('count_all')),
            SUM('size', AS('sum')),
            AVG('size', AS('avg')),
            MIN('size', AS('min')),
            MAX('size', AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) AS count,
            COUNT(*) AS count_all,
            SUM(size) AS sum,
            AVG(size) AS avg,
            MIN(size) AS min,
            MAX(size) AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        ORDER BY city, name`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`groupby 2`, () => {
        const table = `
+----------+------+-------+-----------+------+------+------+------+
| city     | name | count | count_all |  sum |  avg |  min |  max |
+----------+------+-------+-----------+------+------+------+------+
| Berlin   | A    |     1 |         1 |    5 |    5 |    5 |    5 |
| Berlin   | B    |     1 |         1 |    1 |    1 |    1 |    1 |
| Berlin   | B    |     1 |         1 |    2 |    2 |    2 |    2 |
| Berlin   | B    |     1 |         1 |    3 |    3 |    3 |    3 |
| Berlin   | B    |     1 |         1 |    7 |    7 |    7 |    7 |
| Berlin   | C    |     1 |         1 |    1 |    1 |    1 |    1 |
| Berlin   | C    |     1 |         1 |    2 |    2 |    2 |    2 |
| Berlin   | C    |     2 |         2 |    6 |    3 |    3 |    3 |
| Budapest | B    |     0 |         2 | null | null | null | null |
| Budapest | B    |     1 |         1 |    5 |    5 |    5 |    5 |
| Budapest | B    |     1 |         1 |   11 |   11 |   11 |   11 |
| Budapest | C    |     0 |         2 | null | null | null | null |
| Budapest | C    |     1 |         1 |    3 |    3 |    3 |    3 |
| London   | A    |     3 |         3 |   24 |    8 |    8 |    8 |
| London   | A    |     1 |         1 |    9 |    9 |    9 |    9 |
| London   | C    |     0 |         1 | null | null | null | null |
| London   | C    |     1 |         1 |    2 |    2 |    2 |    2 |
+----------+------+-------+-----------+------+------+------+------+
17 rows selected`;
        
        const res =
        SELECT('city', 'name',
            COUNT('size', AS('count')),
            COUNT('*', AS('count_all')),
            SUM('size', AS('sum')),
            AVG('size', AS('avg')),
            MIN('size', AS('min')),
            MAX('size', AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name,
            COUNT(size) AS count,
            COUNT(*) AS count_all,
            SUM(size) AS sum,
            AVG(size) AS avg,
            MIN(size) AS min,
            MAX(size) AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        ORDER BY city, name`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });


    it(`groupby 3`, () => {
        const table = `
+----------+------+-------+-----------+------+-------------------+------+------+
| city     | name | count | count_all |  sum |               avg |  min |  max |
+----------+------+-------+-----------+------+-------------------+------+------+
| Berlin   | A    |     1 |         1 |    5 |                 5 |    5 |    5 |
| Berlin   | C    |     4 |         4 |    9 |              2.25 |    1 |    3 |
| Berlin   | B    |     4 |         4 |   13 |              3.25 |    1 |    7 |
| Budapest | C    |     1 |         3 |    3 |                 3 |    3 |    3 |
| Budapest | B    |     2 |         4 |   16 |                 8 |    5 |   11 |
| London   | C    |     1 |         2 |    2 |                 2 |    2 |    2 |
| London   | A    |     4 |         4 |   33 |              8.25 |    8 |    9 |
| Madrid   | B    |     3 |         3 |   27 |                 9 |    2 |   22 |
| Paris    | A    |     4 |         5 |   19 |              4.75 |    4 |    7 |
| Rome     | B    |     0 |         1 | null |              null | null | null |
| Rome     | A    |     1 |         2 |    4 |                 4 |    4 |    4 |
| Rome     | C    |     3 |         3 |   23 | 7.666666666666667 |    2 |   11 |
+----------+------+-------+-----------+------+-------------------+------+------+
12 rows selected`;
        
        const res =
        SELECT('city', 'name',
            COUNT('size', AS('count')),
            COUNT('*', AS('count_all')),
            SUM('size', AS('sum')),
            AVG('size', AS('avg')),
            MIN('size', AS('min')),
            MAX('size', AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city', 'name'),
        ORDER_BY('city', 'sum'))

        const expected =
        db.prepare(`
        SELECT city, name,
            COUNT(size) AS count,
            COUNT(*) AS count_all,
            SUM(size) AS sum,
            AVG(size) AS avg,
            MIN(size) AS min,
            MAX(size) AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        GROUP BY city, name
        ORDER BY city, sum`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`groupby 4`, () => {
        const table = `
+----------+------+-------+------+
| city     |  add | count |  sum |
+----------+------+-------+------+
| Berlin   | null |     0 | null |
| Berlin   |    3 |     1 |    3 |
| Berlin   |    8 |     1 |    8 |
| Berlin   |    9 |     2 |   18 |
| Berlin   |   10 |     1 |   10 |
| Berlin   |   11 |     1 |   11 |
| Berlin   |   17 |     1 |   17 |
| Berlin   |   20 |     1 |   20 |
| Budapest | null |     0 | null |
| Budapest |    6 |     2 |   12 |
+----------+------+-------+------+
10 rows selected`;
        
        const res =
        SELECT('city',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
            COUNT('add', AS('count')),
            SUM('add', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city', 'add'),
        HAVING(r => r.city < 'L'),
        ORDER_BY('city', 'add'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`groupby 5`, () => {
        const table = `
+----------+-------+------+------+
| city     | count |  sum |  add |
+----------+-------+------+------+
| Berlin   |     0 | null | null |
| Berlin   |     1 |    3 |    3 |
| Berlin   |     1 |    8 |    8 |
| Berlin   |     2 |   18 |    9 |
| Berlin   |     1 |   10 |   10 |
| Berlin   |     1 |   11 |   11 |
| Berlin   |     1 |   17 |   17 |
| Berlin   |     1 |   20 |   20 |
| Budapest |     0 | null | null |
| Budapest |     2 |   12 |    6 |
+----------+-------+------+------+
10 rows selected`;
        
        const res =
        SELECT('city',
            COUNT('add', AS('count')),
            SUM('add', AS('sum')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city', 'add'),
        HAVING(r => r.city < 'L'),
        ORDER_BY('city', 'add'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`groupby 6`, () => {
        const table = `
+----------+-------+------+------+
| city     | count |  sum |  add |
+----------+-------+------+------+
| Berlin   |     0 | null | null |
| Berlin   |     1 |    3 |    3 |
| Berlin   |     1 |    8 |    8 |
| Berlin   |     2 |   18 |    9 |
| Berlin   |     1 |   10 |   10 |
| Berlin   |     1 |   11 |   11 |
| Berlin   |     1 |   17 |   17 |
| Berlin   |     1 |   20 |   20 |
| Budapest |     0 | null | null |
| Budapest |     2 |   12 |    6 |
+----------+-------+------+------+
10 rows selected`;
        
        const res =
        SELECT('city',
            COUNT('add', AS('count')),
            SUM('add', AS('sum')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city', 'add'),
        HAVING(r => r.sum > -1 && r.city < 'L'),
        ORDER_BY('city', 'add'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });



    it(`groupby 7`, () => {
        const table = `
+----------+-------+-----+-----------+
| city     | count | sum | count_sum |
+----------+-------+-----+-----------+
| Berlin   |     8 |  87 |        95 |
| Budapest |     2 |  12 |        14 |
| London   |     3 |  51 |        54 |
| Madrid   |     2 |  36 |        38 |
| Paris    |     3 | 149 |       152 |
| Rome     |     3 |  35 |        38 |
+----------+-------+-----+-----------+
6 rows selected`;
        
        const res =
        SELECT('city',
            COUNT(ADD('t1.id', SUB('t2.age', 'type'), 'size'), AS('count')),
            SUM(ADD('t1.id', SUB('t2.age', 'type'), 'size'), AS('sum')),
            ADD('count', 'sum', AS('count_sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`groupby + having 1`, () => {
        const table = `
+----------+------+------+-------+-----------+------+------+------+------+
| city     | name | size | count | count_all |  sum |  avg |  min |  max |
+----------+------+------+-------+-----------+------+------+------+------+
| Budapest | B    | null |     0 |         2 | null | null | null | null |
| Budapest | B    |    5 |     1 |         1 |    5 |    5 |    5 |    5 |
| Budapest | B    |   11 |     1 |         1 |   11 |   11 |   11 |   11 |
| Budapest | C    | null |     0 |         2 | null | null | null | null |
| Budapest | C    |    3 |     1 |         1 |    3 |    3 |    3 |    3 |
| London   | A    |    8 |     3 |         3 |   24 |    8 |    8 |    8 |
| London   | A    |    9 |     1 |         1 |    9 |    9 |    9 |    9 |
| London   | C    | null |     0 |         1 | null | null | null | null |
| London   | C    |    2 |     1 |         1 |    2 |    2 |    2 |    2 |
+----------+------+------+-------+-----------+------+------+------+------+
9 rows selected`;
        
        const res =
        SELECT('city', 'name', 'size',
            COUNT('size', AS('count')),
            COUNT('*', AS('count_all')),
            SUM('size', AS('sum')),
            AVG('size', AS('avg')),
            MIN('size', AS('min')),
            MAX('size', AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.city > 'Berlin'),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name, size,
            COUNT(size) AS count,
            COUNT(*) AS count_all,
            SUM(size) AS sum,
            AVG(size) AS avg,
            MIN(size) AS min,
            MAX(size) AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        HAVING city > 'Berlin'
        ORDER BY city, name`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`groupby + having 2`, () => {
        const table = `
+----------+------+-------+-----------+-----+-----+-----+-----+
| city     | name | count | count_all | sum | avg | min | max |
+----------+------+-------+-----------+-----+-----+-----+-----+
| Berlin   | A    |     1 |         1 |   5 |   5 |   5 |   5 |
| Berlin   | B    |     1 |         1 |   7 |   7 |   7 |   7 |
| Berlin   | C    |     2 |         2 |   6 |   3 |   3 |   3 |
| Budapest | B    |     1 |         1 |   5 |   5 |   5 |   5 |
| Budapest | B    |     1 |         1 |  11 |  11 |  11 |  11 |
| London   | A    |     3 |         3 |  24 |   8 |   8 |   8 |
| London   | A    |     1 |         1 |   9 |   9 |   9 |   9 |
+----------+------+-------+-----------+-----+-----+-----+-----+
7 rows selected`;
        
        const res =
        SELECT('city', 'name',
            COUNT('size', AS('count')),
            COUNT('*', AS('count_all')),
            SUM('size', AS('sum')),
            AVG('size', AS('avg')),
            MIN('size', AS('min')),
            MAX('size', AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        HAVING(r => r.sum > 3),
        ORDER_BY('city', 'name'))

        const expected =
        db.prepare(`
        SELECT city, name,
            COUNT(size) AS count,
            COUNT(*) AS count_all,
            SUM(size) AS sum,
            AVG(size) AS avg,
            MIN(size) AS min,
            MAX(size) AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE city < 'M'
        GROUP BY city, name, size
        HAVING sum > 3
        ORDER BY city, name`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`groupby nesting 1`, () => {
        const table = `
+----------+-------+-----+
| city     | count | sum |
+----------+-------+-----+
| Berlin   |     8 |  87 |
| Budapest |     2 |  12 |
| London   |     3 |  51 |
| Madrid   |     2 |  36 |
| Paris    |     3 | 149 |
| Rome     |     3 |  35 |
+----------+-------+-----+
6 rows selected`;
        
        const res =
        SELECT('city',
            COUNT(ADD('t1.id', SUB('t2.age', 'type'), 'size'), AS('count')),
            SUM(ADD('t1.id', SUB('t2.age', 'type'), 'size'), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`groupby nesting 2`, () => {
        const table = `
+----------+------+------+
| city     |  add |  sum |
+----------+------+------+
| Berlin   | null | null |
| Berlin   |    3 |    3 |
| Berlin   |    8 |    8 |
| Budapest | null | null |
| London   | null | null |
| Madrid   | null | null |
| Madrid   |    4 |    4 |
| Paris    | null | null |
| Rome     | null | null |
+----------+------+------+
9 rows selected`;
        
        const res =
        SELECT('city',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
            SUM('add', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city', 'add'),
        HAVING(r => r.sum < 10),
        ORDER_BY('city'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });



    it(`groupby nesting 3`, () => {
        const table = `
+----------+-------+-----+-----------+
| city     | count | sum | count_sum |
+----------+-------+-----+-----------+
| Berlin   |     8 |  87 |        95 |
| Budapest |     2 |  12 |        14 |
| London   |     3 |  51 |        54 |
| Madrid   |     2 |  36 |        38 |
| Paris    |     3 | 149 |       152 |
| Rome     |     3 |  35 |        38 |
+----------+-------+-----+-----------+
6 rows selected`;
        
        const res =
        SELECT('city',
            COUNT(ADD('t1.id', SUB('t2.age', 'type'), 'size'), AS('count')),
            SUM(ADD('t1.id', SUB('t2.age', 'type'), 'size'), AS('sum')),
            ADD('count', 'sum', AS('count_sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`1`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column size (SELECT 3) does not exist in GROUP_BY'));
    });

    it(`2`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column add (SELECT 4) does not exist in GROUP_BY'));
    });

    it(`3`, () => {
        const query = () =>
        SELECT('city', 'name', 'size',
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'name', 'size', 'sizexxx'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column sizexxx (GROUP_BY) does not exist'));
    });

    it(`4`, () => {
        const query = () =>
        SELECT('city',
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city'),
        ORDER_BY('city', 'name'))

        expect(query).toThrow(new Error('column name (ORDER_BY) does not exist'));
    });

    it(`5`, () => {
        const query = () =>
        SELECT('city',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'add'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('column add (SELECT 3 AS) already exists'));
    });

    it(`6`, () => {
        const query = () =>
        SELECT('city',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('city')),
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'add'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('column city (SELECT 3 AS) already exists'));
    });

    it(`7`, () => {
        const query = () =>
        SELECT('city',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('id')),
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.city < 'M'),
        GROUP_BY('city', 'add','id'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('column id (SELECT 3 AS) already exists [in table]'));
    });



    it(`8`, () => {
        const query = () =>
        SELECT('city',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add2')),
            ADD('add', 'add2', AS('add3')),
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city', 'add', 'add2'))

        expect(query).toThrow(new Error('column add3 (SELECT 4) does not exist in GROUP_BY'));
    });

    it(`9`, () => {
        const query = () =>
        SELECT('city',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add2')),
            ADD('add', 'add2', AS('add3')),
            COUNT('size', AS('count')),
            SUM('size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city', 'add', 'add2', 'add3'),
        HAVING(r => r.size > 0),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('column size (HAVING) does not exist'));
    });

    it(`nesting 1`, () => {
        const query = () =>
        SELECT('city',
            SUM('size', AS('sum')),
            SUM(SUM('size'), AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('nesting error (SELECT 3)'));
    });

    it(`nesting 2`, () => {
        const query = () =>
        SELECT('city',
            SUM('size', AS('sum')),
            SUM('sum', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('nesting error (SELECT 3 sum)'));
    });

    it(`nesting 3`, () => {
        const query = () =>
        SELECT('city',
            SUM(ADD('t1.id', SUB('t2.agexxx', 'type'), 'size'), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'))

        expect(query).toThrow(new Error('column t2.agexxx (SELECT 2) does not exist'));
    });

    it(`nesting 4`, () => {
        const query = () =>
        SELECT('city',
            SUM('size', AS('sum')),
            SUM('sum', AS('sum2')),
            ADD('sum', 'city', AS('sum_sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('nesting error (SELECT 3 sum)'));
    });

    it(`1`, () => {
        const query = () =>
        SELECT('city',
            SUM('size', AS('sum')),
            SUM('type', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city', 'sum'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('column sum (GROUP_BY) does not exist'));
    });

    it(`1`, () => {
        const query = () =>
        SELECT('city',
            SUM(AS('sum')),
            SUM('type', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('more args expected (SELECT 2)'));
    });

    it(`1`, () => {
        const query = () =>
        SELECT('city',
            SUM_DISTINCT(AS('sum')),
            SUM('type', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('more args expected (SELECT 2)'));
    });


    it(`1`, () => {
        const query = () =>
        SELECT('city',
            SUM({}, AS('sum')),
            SUM('type', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('invalid parameter (SELECT 2)'));
    });

    it(`1`, () => {
        const query = () =>
        SELECT('city',
            SUM(1, AS('sum')),
            SUM('type', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('invalid parameter (SELECT 2)'));
    });

    it(`1`, () => {
        const query = () =>
        SELECT('city',
            SUM(1, 1, AS('sum')),
            SUM('type', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('too many args (SELECT 2)'));
    });

    it(`1`, () => {
        const query = () =>
        SELECT('city',
            SUM(ADD('size', 'type'), 1, AS('sum')),
            SUM('type', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('too many args (SELECT 2)'));
    });

    it(`1`, () => {
        const query = () =>
        SELECT('city',
            SUM(1, ADD('size', 'type'), AS('sum')),
            SUM('type', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        GROUP_BY('city'),
        ORDER_BY('city'))

        expect(query).toThrow(new Error('too many args (SELECT 2)'));
    });

});

