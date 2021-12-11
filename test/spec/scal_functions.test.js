import { SELECT, FROM, JOIN, ON, ORDER_BY, ADD, AS, WHERE, SUB, MUL, createFn, DIV, ID } from '../../dist/select.js'
import { input4, input5, createTable } from './data.js'
import { drawTable } from './utils.js'

const trim = str => str.trim();

import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const sqlite = require('better-sqlite3');
const db = sqlite(':memory:');


createTable(db, input4, 'input4')
createTable(db, input5, 'input5')



describe('scalar functions:', () => {
    it(`starting point`, () => {
        const table = `
+-------+------+--------+------+-------+----------+--------+------+
| t1.id | name | t1.age | type | t2.id | city     | t2.age | size |
+-------+------+--------+------+-------+----------+--------+------+
|     2 | B    |      3 |    3 |     2 | Berlin   |   null |    7 |
|     2 | B    |      3 |    3 |     2 | Berlin   |      3 |    1 |
|     2 | B    |      3 |    3 |     2 | Berlin   |      8 |    2 |
|     2 | B    |      3 |    3 |     2 | Berlin   |      8 |    3 |
|     3 | C    |      5 |    4 |     3 | Berlin   |      8 |    1 |
|     3 | C    |      5 |    4 |     3 | Berlin   |      8 |    2 |
|     3 | C    |      5 |    4 |     3 | Berlin   |      9 |    3 |
|     1 | A    |      8 |    1 |     1 | Berlin   |     15 |    5 |
|     3 | C    |      5 |    4 |     3 | Berlin   |     15 |    3 |
|     2 | B    |      3 |    3 |     2 | Budapest |   null |   11 |
|     2 | B    |      3 |    3 |     2 | Budapest |      2 |    5 |
|     3 | C    |      5 |    4 |     3 | Budapest |      2 | null |
|     3 | C    |      5 |    4 |     3 | Budapest |      3 | null |
|     3 | C    |      5 |    4 |     3 | Budapest |      4 |    3 |
|     2 | B    |      3 |    3 |     2 | Budapest |      5 | null |
|     2 | B    |      3 |    3 |     2 | Budapest |      7 | null |
|     1 | A    |      8 |    1 |     1 | London   |   null |    8 |
|     3 | C    |      5 |    4 |     3 | London   |   null |    2 |
|     1 | A    |      8 |    1 |     1 | London   |      1 |    9 |
|     3 | C    |      5 |    4 |     3 | London   |      2 | null |
|     1 | A    |      8 |    1 |     1 | London   |     10 |    8 |
|     1 | A    |      8 |    1 |     1 | London   |     15 |    8 |
|     2 | B    |      3 |    3 |     2 | Madrid   |   null |    2 |
|     2 | B    |      3 |    3 |     2 | Madrid   |      2 |    3 |
|     2 | B    |      3 |    3 |     2 | Madrid   |     11 |   22 |
|     1 | A    |      8 |    1 |     1 | Paris    |   null |    7 |
|     1 | A    |      8 |    1 |     1 | Paris    |      8 | null |
|     1 | A    |      8 |    1 |     1 | Paris    |     19 |    4 |
|     1 | A    |      8 |    1 |     1 | Paris    |     19 |    4 |
|     1 | A    |      8 |    1 |     1 | Paris    |     99 |    4 |
|     3 | C    |      5 |    4 |     3 | Rome     |   null |    2 |
|     2 | B    |      3 |    3 |     2 | Rome     |      1 | null |
|     3 | C    |      5 |    4 |     3 | Rome     |      1 |   11 |
|     3 | C    |      5 |    4 |     3 | Rome     |      2 |   10 |
|     1 | A    |      8 |    1 |     1 | Rome     |      9 |    4 |
|     1 | A    |      8 |    1 |     1 | Rome     |     33 | null |
+-------+------+--------+------+-------+----------+--------+------+
36 rows selected`;
        
        const res =
        SELECT('*',
        FROM(input4, JOIN, input5, ON('id', 'id')),
        ORDER_BY('city', 't2.age'))

        const expected =
        db.prepare(`
        SELECT t1.id AS 't1.id', name, t1.age AS 't1.age', type, t2.id AS 't2.id', city, t2.age AS 't2.age', size
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        ORDER BY city, t2.age`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`simple scalar function: add`, () => {
        const table = `
+-------+------+--------+------+------+------+
| t1.id | name | t2.age | type | size |  sum |
+-------+------+--------+------+------+------+
|     3 | C    |      8 |    4 |    1 |   20 |
|     3 | C    |      8 |    4 |    2 |   21 |
|     3 | C    |      9 |    4 |    3 |   23 |
|     3 | C    |     15 |    4 |    3 |   29 |
|     3 | C    |      2 |    4 | null | null |
|     3 | C    |      3 |    4 | null | null |
|     3 | C    |      4 |    4 |    3 |   18 |
|     3 | C    |   null |    4 |    2 | null |
|     3 | C    |      2 |    4 | null | null |
|     3 | C    |   null |    4 |    2 | null |
|     3 | C    |      1 |    4 |   11 |   23 |
|     3 | C    |      2 |    4 |   10 |   23 |
+-------+------+--------+------+------+------+
12 rows selected`;
        
        const res =
        SELECT('t1.id', 'name', 't2.age', 'type', 'size',
            ADD('t1.id', 't2.age', 1, 'type', 3, 'size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3 && (r.sum == null || r.sum > 0)),
        ORDER_BY('city', 't2.age'))

        const expected =
        db.prepare(`
        SELECT t1.id AS 't1.id', name, t2.age AS 't2.age', type, size,
            t1.id + t2.age + 1 + type + 3 + size AS sum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY city, t2.age`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`nesting: add + sub`, () => {
        const table = `
+-------+------+--------+------+------+------+
| t1.id | name | t2.age | type | size |  sum |
+-------+------+--------+------+------+------+
|     3 | C    |      8 |    4 |    1 |    8 |
|     3 | C    |      8 |    4 |    2 |    9 |
|     3 | C    |      9 |    4 |    3 |   11 |
|     3 | C    |     15 |    4 |    3 |   17 |
|     3 | C    |      2 |    4 | null | null |
|     3 | C    |      3 |    4 | null | null |
|     3 | C    |      4 |    4 |    3 |    6 |
|     3 | C    |   null |    4 |    2 | null |
|     3 | C    |      2 |    4 | null | null |
|     3 | C    |   null |    4 |    2 | null |
|     3 | C    |      1 |    4 |   11 |   11 |
|     3 | C    |      2 |    4 |   10 |   11 |
+-------+------+--------+------+------+------+
12 rows selected`;
        
        const res =
        SELECT('t1.id', 'name', 't2.age', 'type', 'size',
            ADD('t1.id', SUB('t2.age', 'type'), 'size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('city', 't2.age', 'sum'))

        const expected =
        db.prepare(`
        SELECT t1.id AS 't1.id', name, t2.age AS 't2.age', type, size,
            t1.id + (t2.age - type) + size AS sum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY city, t2.age`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`deep nesting: add + sub 1`, () => {
        const table = `
+-------+------+--------+------+------+------+
| t1.id | name | t2.age | type | size |  sum |
+-------+------+--------+------+------+------+
|     3 | C    |      8 |    4 |    1 |   13 |
|     3 | C    |      8 |    4 |    2 |   14 |
|     3 | C    |      9 |    4 |    3 |   16 |
|     3 | C    |     15 |    4 |    3 |   22 |
|     3 | C    |      2 |    4 | null | null |
|     3 | C    |      3 |    4 | null | null |
|     3 | C    |      4 |    4 |    3 |   11 |
|     3 | C    |   null |    4 |    2 | null |
|     3 | C    |      2 |    4 | null | null |
|     3 | C    |   null |    4 |    2 | null |
|     3 | C    |      1 |    4 |   11 |   16 |
|     3 | C    |      2 |    4 |   10 |   16 |
+-------+------+--------+------+------+------+
12 rows selected`;
        
        const res =
        SELECT('t1.id', 'name', 't2.age', 'type', 'size',
            ADD('t1.id', SUB(ADD('t1.age', 't2.age'), 'type'), 'size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('city', 't2.age'))

        const expected =
        db.prepare(`
        SELECT t1.id AS 't1.id', name, t2.age AS 't2.age', type, size,
            t1.id + ((t1.age + t2.age) - type) + size AS sum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY city, t2.age`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`deep nesting: add + sub 2`, () => {
        const table = `
+-------+------+--------+------+------+------+
| t1.id | name | t2.age | type | size |  sum |
+-------+------+--------+------+------+------+
|     3 | C    |      8 |    4 |    1 |   12 |
|     3 | C    |      8 |    4 |    2 |   12 |
|     3 | C    |      9 |    4 |    3 |   13 |
|     3 | C    |     15 |    4 |    3 |   19 |
|     3 | C    |      2 |    4 | null | null |
|     3 | C    |      3 |    4 | null | null |
|     3 | C    |      4 |    4 |    3 |    8 |
|     3 | C    |   null |    4 |    2 | null |
|     3 | C    |      2 |    4 | null | null |
|     3 | C    |   null |    4 |    2 | null |
|     3 | C    |      1 |    4 |   11 |    5 |
|     3 | C    |      2 |    4 |   10 |    6 |
+-------+------+--------+------+------+------+
12 rows selected`;
        
        const res =
        SELECT('t1.id', 'name', 't2.age', 'type', 'size',
            ADD('t1.id', SUB(ADD('t1.age', 't2.age'), ADD('type', 'size')), 'size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('city', 't2.age'))

        const expected =
        db.prepare(`
        SELECT t1.id AS 't1.id', name, t2.age AS 't2.age', type, size,
            t1.id + ((t1.age + t2.age) - (type + size)) + size AS sum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY city, t2.age`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`deep nesting: add + sub 2`, () => {
        const table = `
+-------+------+--------+------+------+------+------+--------+
| t1.id | name | t2.age | type | size |  sum | sum2 | sumsum |
+-------+------+--------+------+------+------+------+--------+
|     3 | C    |      8 |    4 |    1 |   12 |   24 |     36 |
|     3 | C    |      8 |    4 |    2 |   12 |   24 |     36 |
|     3 | C    |      9 |    4 |    3 |   13 |   26 |     39 |
|     3 | C    |     15 |    4 |    3 |   19 |   38 |     57 |
|     3 | C    |      2 |    4 | null | null | null |   null |
|     3 | C    |      3 |    4 | null | null | null |   null |
|     3 | C    |      4 |    4 |    3 |    8 |   16 |     24 |
|     3 | C    |   null |    4 |    2 | null | null |   null |
|     3 | C    |      2 |    4 | null | null | null |   null |
|     3 | C    |   null |    4 |    2 | null | null |   null |
|     3 | C    |      1 |    4 |   11 |    5 |   10 |     15 |
|     3 | C    |      2 |    4 |   10 |    6 |   12 |     18 |
+-------+------+--------+------+------+------+------+--------+
12 rows selected`;
        
        const res =
        SELECT('t1.id', 'name', 't2.age', 'type', 'size',
            ADD('t1.id', SUB(ADD('t1.age', 't2.age'), ADD('type', 'size')), 'size', AS('sum')),
            ADD('sum', 'sum', AS('sum2')),
            ADD('sum', 'sum2', AS('sumsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('city', 't2.age'))

        const expected =
        db.prepare(`
        SELECT t1.id AS 't1.id', name, t2.age AS 't2.age', type, size,
            t1.id + ((t1.age + t2.age) - (type + size)) + size AS sum,
            t1.id + ((t1.age + t2.age) - (type + size)) + size  +
            t1.id + ((t1.age + t2.age) - (type + size)) + size  AS sum2,
            t1.id + ((t1.age + t2.age) - (type + size)) + size  +
            t1.id + ((t1.age + t2.age) - (type + size)) + size  +
            t1.id + ((t1.age + t2.age) - (type + size)) + size AS sumsum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY city, t2.age`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`extreme deep nesting: add + sub`, () => {
        const table = `
+-------+------+--------+------+------+------+
| t1.id | name | t2.age | type | size |  sum |
+-------+------+--------+------+------+------+
|     3 | C    |      8 |    4 |    1 |   97 |
|     3 | C    |      8 |    4 |    2 |  105 |
|     3 | C    |      9 |    4 |    3 |  127 |
|     3 | C    |     15 |    4 |    3 |  211 |
|     3 | C    |      2 |    4 | null | null |
|     3 | C    |      3 |    4 | null | null |
|     3 | C    |      4 |    4 |    3 |   57 |
|     3 | C    |   null |    4 |    2 | null |
|     3 | C    |      2 |    4 | null | null |
|     3 | C    |   null |    4 |    2 | null |
|     3 | C    |      1 |    4 |   11 |   23 |
|     3 | C    |      2 |    4 |   10 |   43 |
+-------+------+--------+------+------+------+
12 rows selected`;
        
        const res =
        SELECT('t1.id', 'name', 't2.age', 'type', 'size',
            ADD('t1.id', SUB(ADD('t1.age', 't2.age', MUL(ADD('t1.age', 'size', 5), 't2.age')), ADD('type', 'size', 3)), 'size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('city', 't2.age'))

        const expected =
        db.prepare(`
        SELECT t1.id AS 't1.id', name, t2.age AS 't2.age', type, size,
            t1.id + ((t1.age + t2.age + ((t1.age + size + 5) * t2.age)) - (type + size + 3)) + size AS sum
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY city, t2.age`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`simple scalar function: add -> string concat`, () => {
        const table = `
+-------+------+----------+-----------------------+
| t1.id | name | city     | res                   |
+-------+------+----------+-----------------------+
|     3 | C    | Berlin   | C -> city -> Berlin   |
|     3 | C    | Berlin   | C -> city -> Berlin   |
|     3 | C    | Berlin   | C -> city -> Berlin   |
|     3 | C    | Berlin   | C -> city -> Berlin   |
|     3 | C    | Budapest | C -> city -> Budapest |
|     3 | C    | Budapest | C -> city -> Budapest |
|     3 | C    | Budapest | C -> city -> Budapest |
|     3 | C    | London   | C -> city -> London   |
|     3 | C    | London   | C -> city -> London   |
|     3 | C    | Rome     | C -> city -> Rome     |
|     3 | C    | Rome     | C -> city -> Rome     |
|     3 | C    | Rome     | C -> city -> Rome     |
+-------+------+----------+-----------------------+
12 rows selected`;
        
        const res =
        SELECT('t1.id', 'name', 'city',
            ADD('name', ': -> city -> :', 'city', AS('res')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('city', 't2.age'))

        const expected =
        db.prepare(`
        SELECT t1.id AS 't1.id', name, city,
            name || ' -> city -> ' || city AS res
        FROM input4 AS t1 JOIN input5 AS t2 ON(t1.id = t2.id)
        WHERE type > 3
        ORDER BY city, t2.age`).all();

        expect(trim(drawTable(res))).toEqual(trim(table));                          
        expect(res).toEqual(expected);
    });

    it(`custom scalar function 1`, () => {
        const table = `
+-------+------+----------+----------+
| t1.id | name | city     | CITY     |
+-------+------+----------+----------+
|     3 | C    | Berlin   | BERLIN   |
|     3 | C    | Berlin   | BERLIN   |
|     3 | C    | Berlin   | BERLIN   |
|     3 | C    | Berlin   | BERLIN   |
|     3 | C    | Budapest | BUDAPEST |
|     3 | C    | Budapest | BUDAPEST |
|     3 | C    | Budapest | BUDAPEST |
|     3 | C    | London   | LONDON   |
|     3 | C    | London   | LONDON   |
|     3 | C    | Rome     | ROME     |
|     3 | C    | Rome     | ROME     |
|     3 | C    | Rome     | ROME     |
+-------+------+----------+----------+
12 rows selected`;

        const TO_UPPER = createFn(str => str.toUpperCase())
        
        const res =
        SELECT('t1.id', 'name', 'city',
            TO_UPPER('city', AS('CITY')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('CITY', 't2.age'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`custom scalar function 2`, () => {
        const table = `
+-------+------+------+-------------+---------+
| t1.id | size | type | not_rounded | rounded |
+-------+------+------+-------------+---------+
|     3 |    1 |    4 |        0.25 |       0 |
|     3 |    2 |    4 |         0.5 |       1 |
|     3 |    3 |    4 |        0.75 |       1 |
|     3 |    3 |    4 |        0.75 |       1 |
|     3 | null |    4 |        null |       0 |
|     3 | null |    4 |        null |       0 |
|     3 |    3 |    4 |        0.75 |       1 |
|     3 |    2 |    4 |         0.5 |       1 |
|     3 | null |    4 |        null |       0 |
|     3 |    2 |    4 |         0.5 |       1 |
|     3 |   11 |    4 |        2.75 |       3 |
|     3 |   10 |    4 |         2.5 |       3 |
+-------+------+------+-------------+---------+
12 rows selected`;

        const ROUND = createFn(n => Math.round(n))
        
        const res =
        SELECT('t1.id', 'size', 'type',
            DIV('size', 'type', AS('not_rounded')),
            ROUND('not_rounded', AS('rounded')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('city', 't2.age'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });

    it(`custom scalar function 3`, () => {
        const table = `
+-------+------+------+-------------+---------+
| t1.id | size | type | not_rounded | rounded |
+-------+------+------+-------------+---------+
|     3 | null |    4 |        null |       0 |
|     3 | null |    4 |        null |       0 |
|     3 | null |    4 |        null |       0 |
|     3 |    1 |    4 |        0.25 |       0 |
|     3 |    2 |    4 |         0.5 |       1 |
|     3 |    2 |    4 |         0.5 |       1 |
|     3 |    2 |    4 |         0.5 |       1 |
|     3 |    3 |    4 |        0.75 |       1 |
|     3 |    3 |    4 |        0.75 |       1 |
|     3 |    3 |    4 |        0.75 |       1 |
|     3 |   10 |    4 |         2.5 |       3 |
|     3 |   11 |    4 |        2.75 |       3 |
+-------+------+------+-------------+---------+
12 rows selected`;

        const ROUND = createFn(n => Math.round(n))
        
        const res =
        SELECT('t1.id', 'size', 'type',
            DIV('size', 'type', AS('not_rounded')),
            ROUND('not_rounded', AS('rounded')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('not_rounded'))

        expect(trim(drawTable(res))).toEqual(trim(table));                          
    });





});

describe('scalar functions errors:', () => {
    it(``, () => {        
        const query = () =>
        SELECT('t1.id', 'name', 't2.age', 'type', 'size',
            ADD('t1.id', SUB(ADD('t1.age', 't2.age'), ADD('type', 'size')), 'size', AS('sum')),
            ADD('t1.age', 'sum2', AS('add')),
            ADD('sum', 'sum', AS('sum2')),
            ADD('sum', 'sum2', AS('sumsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('city', 't2.age'))

        expect(query).toThrow(new Error('column sum2 (SELECT 7) does not exist'));
    });

    it(``, () => {        
        const query = () =>
        SELECT('t1.id', 'name', 't2.age', 'type', 'size',
            ADD('t1.id', SUB(ADD('t1.age', 't2.age'), ADD('type', 'size')), 'size', AS('sum')),
            ADD('sum', 'sum'),
            ADD('sum', 'sum2', AS('sumsum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('city', 't2.age'))

        expect(query).toThrow(new Error('AS expected (SELECT 7)'));
    });

    it(``, () => {        
        const query = () =>
        SELECT('t1.id', 'name', 't2.age', 'type', 'size',
            ADD('t1.id', SUB(ADD('t1.age', 't2.age'), ADD('typexxx', 'size')), 'size', AS('sum')),
        FROM(input4, JOIN, input5, ON('id', 'id')),
        WHERE(r => r.type > 3),
        ORDER_BY('city', 't2.age'))

        expect(query).toThrow(new Error('column typexxx (SELECT 6) does not exist'));
    });



});

