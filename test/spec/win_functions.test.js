import { SELECT, FROM, JOIN, ON, ORDER_BY, ADD, AS, WHERE, OVER,
    SUB, COUNT, SUM, AVG, MIN, MAX,
    SELECT_TOP, DESC, HAVING, PARTITION_BY } from '../../dist/select.js'
import { input4, input5, createTable } from './data.js'
import { drawTable } from './utils.js'
const trim = str => str.trim();

import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const sqlite = require('better-sqlite3');
const db = sqlite(':memory:');


createTable(db, input4, 'input4')
createTable(db, input5, 'input5')



describe('window functions (without grouping):', () => {
    it(`window functions 1`, () => {
        const table = `
+-------+-----------+-----+-------------------+-----+-----+
| count | count_all | sum |               avg | min | max |
+-------+-----------+-----+-------------------+-----+-----+
|     9 |        12 |  37 | 4.111111111111111 |   1 |  11 |
|     9 |        12 |  37 | 4.111111111111111 |   1 |  11 |
|     9 |        12 |  37 | 4.111111111111111 |   1 |  11 |
|     9 |        12 |  37 | 4.111111111111111 |   1 |  11 |
|     9 |        12 |  37 | 4.111111111111111 |   1 |  11 |
+-------+-----------+-----+-------------------+-----+-----+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            COUNT('size', OVER(), AS('count')),
            COUNT('*', OVER(), AS('count_all')),
            SUM('size', OVER(), AS('sum')),
            AVG('size', OVER(), AS('avg')),
            MIN('size', OVER(), AS('min')),
            MAX('size', OVER(), AS('max')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        const expected =
        db.prepare(`
        SELECT
            COUNT(size) OVER() AS count,
            COUNT(*) OVER() AS count_all,
            SUM(size) OVER() AS sum,
            AVG(size) OVER() AS avg,
            MIN(size) OVER() AS min,
            MAX(size) OVER() AS max
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum
        LIMIT 5`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`window functions nesting 1`, () => {
        const table = `
+-----+
| sum |
+-----+
|  73 |
|  73 |
|  73 |
|  73 |
|  73 |
+-----+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            SUM(ADD('t1.id', SUB('t2.age', 'type'), 'size'), OVER(), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        const expected =
        db.prepare(`
        SELECT
            SUM(t1.id + (t2.age - type) + size) OVER() AS sum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum
        LIMIT 5`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`window + scalar functions 1`, () => {
        const table = `
+------+-----+
|  add | sum |
+------+-----+
| null |  73 |
| null |  73 |
|    8 |  73 |
|   11 |  73 |
|   11 |  73 |
+------+-----+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
            SUM(ADD('t1.id', SUB('t2.age', 'type'), 'size'), OVER(), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`window + scalar functions 2`, () => {
        const table = `
+------+-----+------+
|  add | sum | sum2 |
+------+-----+------+
| null |  73 | null |
| null |  73 | null |
|    8 |  73 |   81 |
|   11 |  73 |   84 |
|   11 |  73 |   84 |
+------+-----+------+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
            SUM(ADD('t1.id', SUB('t2.age', 'type'), 'size'), OVER(), AS('sum')),
            ADD('add', 'sum', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`window + scalar functions 3`, () => {
        const table = `
+-----+------+------+
| sum | sum2 |  add |
+-----+------+------+
|  73 | null | null |
|  73 | null | null |
|  73 |   81 |    8 |
|  73 |   84 |   11 |
|  73 |   84 |   11 |
+-----+------+------+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            SUM(ADD('t1.id', SUB('t2.age', 'type'), 'size'), OVER(), AS('sum')),
            ADD('add', 'sum', AS('sum2')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`window + scalar functions 4`, () => {
        const table = `
+-----+------+-----+
| sum | sum2 | add |
+-----+------+-----+
|  73 |   90 |  17 |
|  73 |   84 |  11 |
|  73 |   84 |  11 |
|  73 |   84 |  11 |
|  73 |   82 |   9 |
+-----+------+-----+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            SUM('add', OVER(), AS('sum')),
            ADD('add', 'sum', AS('sum2')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('add', DESC, 'sum', 'sum2'))

        expect(trim(drawTable(res))).toEqual(trim(table));
    });

    it(`window functions nesting 1`, () => {
        const table = `
+------+------+------+
| sum1 | sum2 | sum3 |
+------+------+------+
|   37 |   48 |   85 |
|   37 |   48 |   85 |
|   37 |   48 |   85 |
|   37 |   48 |   85 |
|   37 |   48 |   85 |
+------+------+------+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            SUM('size', OVER(), AS('sum1')),
            SUM('type', OVER(), AS('sum2')),
            ADD('sum1', 'sum2', AS('sum3')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum1'))

        const expected =
        db.prepare(`
        SELECT
            SUM(size) OVER() AS sum1,
            SUM(type) OVER() AS sum2,
            SUM(size) OVER() + SUM(type) OVER() AS sum3
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum1
        LIMIT 5`).all();

        expect(trim(drawTable(res))).toEqual(trim(table)); 
        expect(res).toEqual(expected);                         
    });

    it(`window functions nesting 2`, () => {
        const table = `
+------+------+------+------+
| sum1 | sum2 | sum3 | sum4 |
+------+------+------+------+
|   37 |   48 |   85 |  122 |
|   37 |   48 |   85 |  122 |
|   37 |   48 |   85 |  122 |
|   37 |   48 |   85 |  122 |
|   37 |   48 |   85 |  122 |
+------+------+------+------+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            SUM('size', OVER(), AS('sum1')),
            SUM('type', OVER(), AS('sum2')),
            ADD('sum1', 'sum2', AS('sum3')),
            ADD('sum3', 'sum1', AS('sum4')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum1'))

        const expected =
        db.prepare(`
        SELECT
            SUM(size) OVER() AS sum1,
            SUM(type) OVER() AS sum2,
            SUM(size) OVER() + SUM(type) OVER() AS sum3,
            SUM(size) OVER() + SUM(type) OVER() + SUM(size) OVER() AS sum4
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum1
        LIMIT 5`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));  
        expect(res).toEqual(expected);                                                
    });

    it(`window functions nesting 3`, () => {
        const table = `
+-----+
| sum |
+-----+
|  85 |
|  85 |
|  85 |
|  85 |
|  85 |
+-----+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            ADD(SUM('size', OVER()), SUM('type', OVER()), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        const expected =
        db.prepare(`
        SELECT
            SUM(size) OVER() + SUM(type) OVER() AS sum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum
        LIMIT 5`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));          
        expect(res).toEqual(expected);
    });

    it(`window functions nesting 4`, () => {
        const table = `
+------+------+
| sum1 | sum2 |
+------+------+
|   37 |   85 |
|   37 |   85 |
|   37 |   85 |
|   37 |   85 |
|   37 |   85 |
+------+------+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            SUM('size', OVER(), AS('sum1')),
            ADD('sum1', SUM('type', OVER()), AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum1'))

        const expected =
        db.prepare(`
        SELECT
            SUM(size) OVER() AS sum1,
            SUM(size) OVER() + SUM(type) OVER() AS sum2
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum1
        LIMIT 5`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));          
        expect(res).toEqual(expected);
    });

    it(`window functions nesting 5`, () => {
        const table = `
+------+------+
| sum1 | sum2 |
+------+------+
|   37 | null |
|   37 | null |
|   37 | null |
|   37 |   38 |
|   37 |   39 |
+------+------+
5 rows selected`;
               
        const res =
        SELECT_TOP(5)(
            SUM('size', OVER(), AS('sum1')),
            ADD('size', 'sum1', AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum1', 'sum2'))

        const expected =
        db.prepare(`
        SELECT
            SUM(size) OVER() AS sum1,
            size + (SUM(size) OVER()) AS sum2
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum1, sum2
        LIMIT 5`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));          
        expect(res).toEqual(expected);
    });

    it(`window functions nesting 5`, () => {
        const table = `
+------+
|  sum |
+------+
| null |
| null |
| null |
|   38 |
|   39 |
+------+
5 rows selected`;
               
        const res =
        SELECT_TOP(5)(
            ADD('size', SUM('size', OVER()), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        const expected =
        db.prepare(`
        SELECT
            size + (SUM(size) OVER()) AS sum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum
        LIMIT 5`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));          
        expect(res).toEqual(expected);
    });

    it(`window functions 1`, () => {
        const query = () =>
        SELECT(
            SUM('size', OVER(), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.sum > 0),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('column sum (WHERE) does not exist'));
    });

    it(`window functions 2`, () => {
        const query = () =>
        SELECT(
            SUM('size', OVER(), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 0),
        HAVING(r => r.type > 0),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('GROUP_BY required [before HAVING]'));
    });

    it(`1`, () => {
        const query = () =>
        SELECT(
            COUNT('sizexxx', OVER(), AS('count')),
            SUM('size', OVER(), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('column sizexxx (SELECT 1) does not exist'));
    });

    it(`2`, () => {
        const query = () =>
        SELECT(
            COUNT('id', OVER(), AS('count')),
            SUM('size', OVER(), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('column id (SELECT 1) ambiguous'));
    });

    it(`3`, () => {
        const query = () =>
        SELECT(
            COUNT('size', OVER(), AS('count')),
            SUM('size', OVER()),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('AS expected (SELECT 2)'));
    });

    it(`4`, () => {
        const query = () =>
        SELECT(
            SUM('', OVER(), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('invalid parameter (SELECT 1)'));
    });

    it(`5`, () => {
        const query = () =>
        SELECT(
            SUM({}, OVER(), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('invalid parameter (SELECT 1)'));
    });

    it(`nesting`, () => {
        const query = () =>
        SELECT(
            COUNT('size', OVER(), AS('count')),
            SUM(ADD('t1.id', SUB('t2.age', 'typexxx'), 'size'), OVER(), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('column typexxx (SELECT 2) does not exist'));
    });

    it(`7`, () => {
        const query = () =>
        SELECT(
            SUM(SUM('size', OVER()), OVER(), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('nesting error (SELECT 1)'));
    });

    it(`8`, () => {
        const query = () =>
        SELECT(
            SUM(ADD('t1.id', SUB(SUM('size', OVER()), 'type'), 'size'), OVER(), AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('nesting error (SELECT 1)'));
    });

    it(`9`, () => {
        const query = () =>
        SELECT(
            SUM('size', OVER(), AS('sum1')),
            SUM('sum1', OVER(), AS('sum2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        ORDER_BY('sum'))

        expect(query).toThrow(new Error('nesting error (SELECT 2 sum1)'));
    });

    it(`10`, () => {        
        const query = () =>
        SELECT(
            SUM('size', OVER(), AS('sum1')),
            SUM('type', OVER(), AS('sum2')),
            ADD('sum3', 'sum1', AS('sum4')),
            ADD('sum1', 'sum2', AS('sum3')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum1'))

        expect(query).toThrow(new Error('column sum3 (SELECT 3) does not exist'));
    });

});


describe('window functions (OVER/partitionby/orderby):', () => {
    it(`window functions partitionby 1`, () => {
        const table = `
+------+-------+-----+-----+------+
| add1 | count | sum | max | add2 |
+------+-------+-----+-----+------+
|   17 |     9 |  37 |  11 |   17 |
|   11 |     9 |  37 |  11 |   11 |
|   11 |     9 |  37 |  11 |   11 |
|   11 |     9 |  37 |  11 |   11 |
|    9 |     9 |  37 |  11 |    9 |
+------+-------+-----+-----+------+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add1')),
            COUNT('size', OVER(), AS('count')),
            SUM('size', OVER(PARTITION_BY('name')), AS('sum')),
            MAX('size', OVER(), AS('max')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum', 'add1', DESC))

        const expected =
        db.prepare(`
        SELECT
            t1.id + (t2.age - type) + size AS add1,
            COUNT(size) OVER() AS count,
            SUM(size) OVER(PARTITION BY(name)) AS sum,
            MAX(size) OVER() AS max,
            t1.id + (t2.age - type) + size AS add2
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum, add1 DESC
        LIMIT 5`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`window functions partitionby 2`, () => {
        const table = `
+------+-------+-----+-----+------+
| add1 | count | sum | max | add2 |
+------+-------+-----+-----+------+
|    8 |     9 |   1 |  11 |    8 |
|    9 |     9 |   2 |  11 |    9 |
|   17 |     9 |   3 |  11 |   17 |
|    6 |     9 |   3 |  11 |    6 |
| null |     9 |   4 |  11 | null |
+------+-------+-----+-----+------+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add1')),
            COUNT('size', OVER(), AS('count')),
            SUM('size', OVER(PARTITION_BY('add1')), AS('sum')),
            MAX('size', OVER(), AS('max')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum', 'add1', DESC))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`window functions orderby 1`, () => {
        const table = `
+------+-------+-----+-----+------+
| add1 | count | sum | max | add2 |
+------+-------+-----+-----+------+
|   17 |     9 |  37 |  11 |   17 |
|   11 |     9 |  37 |  11 |   11 |
|   11 |     9 |  37 |  11 |   11 |
|   11 |     9 |  37 |  11 |   11 |
|    9 |     9 |  37 |  11 |    9 |
+------+-------+-----+-----+------+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add1')),
            COUNT('size', OVER(), AS('count')),
            SUM('size', OVER(ORDER_BY('name')), AS('sum')),
            MAX('size', OVER(), AS('max')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum', 'add1', DESC))

        const expected =
        db.prepare(`
        SELECT
            t1.id + (t2.age - type) + size AS add1,
            COUNT(size) OVER() AS count,
            SUM(size) OVER(ORDER BY(name)) AS sum,
            MAX(size) OVER() AS max,
            t1.id + (t2.age - type) + size AS add2
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY sum, add1 DESC
        LIMIT 5`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`window functions orderby 2`, () => {
        const table = `
+------+-------+-----+-----+------+
| add1 | count | sum | max | add2 |
+------+-------+-----+-----+------+
| null |     9 |   4 |  11 | null |
| null |     9 |   4 |  11 | null |
| null |     9 |   4 |  11 | null |
| null |     9 |   4 |  11 | null |
| null |     9 |   4 |  11 | null |
+------+-------+-----+-----+------+
5 rows selected`;
        
        const res =
        SELECT_TOP(5)(
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add1')),
            COUNT('size', OVER(), AS('count')),
            SUM('size', OVER(ORDER_BY('add1')), AS('sum')),
            MAX('size', OVER(), AS('max')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum', 'add1', DESC))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`window functions partitionby 1`, () => {        
        const query = () =>
        SELECT_TOP(5)(
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add1')),
            COUNT('size', OVER(), AS('count')),
            SUM('size', OVER(PARTITION_BY('namexxx')), AS('sum')),
            MAX('size', OVER(), AS('max')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum', 'add1', DESC))

        expect(query).toThrow(new Error('column namexxx (PARTITION_BY 3) does not exist'));
    });

    it(`window functions partitionby 2`, () => {        
        const query = () =>
        SELECT_TOP(5)(
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add1')),
            COUNT('size', OVER(), AS('count')),
            SUM('size', OVER(PARTITION_BY('max')), AS('sum')),
            MAX('size', OVER(), AS('max')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum', 'add1', DESC))

        expect(query).toThrow(new Error('column max (PARTITION_BY 3) does not exist'));
    });

    it(`window functions partitionby 3`, () => {
        const query = () =>
        SELECT_TOP(5)(
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add1')),
            COUNT('size', OVER(), AS('count')),
            SUM('size', OVER(PARTITION_BY('count')), AS('sum')),
            MAX('size', OVER(), AS('max')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum', 'add1', DESC))

        expect(query).toThrow(new Error('column count (PARTITION_BY 3) does not exist'));
    });


    it(`window functions orderby 1`, () => {        
        const query = () =>
        SELECT_TOP(5)(
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add1')),
            COUNT('size', OVER(), AS('count')),
            SUM('size', OVER(ORDER_BY('namexxx')), AS('sum')),
            MAX('size', OVER(), AS('max')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum', 'add1', DESC))

        expect(query).toThrow(new Error('column namexxx (ORDER_BY 3) does not exist'));
    });

    it(`window functions orderby 1`, () => {        
        const query = () =>
        SELECT_TOP(5)(
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add1')),
            COUNT('size', OVER(), AS('count')),
            SUM('size', OVER(ORDER_BY('max')), AS('sum')),
            MAX('size', OVER(), AS('max')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum', 'add1', DESC))

        expect(query).toThrow(new Error('column max (ORDER_BY 3) does not exist'));
    });

    it(`window functions orderby 3`, () => {
        const query = () =>
        SELECT_TOP(5)(
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add1')),
            COUNT('size', OVER(), AS('count')),
            SUM('size', OVER(ORDER_BY('count')), AS('sum')),
            MAX('size', OVER(), AS('max')),
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('add2')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('sum', 'add1', DESC))

        expect(query).toThrow(new Error('column count (ORDER_BY 3) does not exist'));
    });


});


