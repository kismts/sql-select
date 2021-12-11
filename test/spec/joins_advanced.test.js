import { SELECT, JOIN, FROM, ON, USING, CROSS_JOIN, WHERE, ORDER_BY, DESC } from '../../dist/select.js'
import { input1, input2, input3, input11, input22, input33 } from './data.js'
import { drawTable } from './utils.js'
const trim = str => str.trim();


describe('joins_2 -> inner joins -> 3 or more inputs:', () => {
    describe('with ON:', () => {
        it(`asterisk - 3 inputs`, () => {
            const table = `
+-------+------+-------+-----+-------+--------+
| t1.id | name | t2.id | age | t3.id | city   |
+-------+------+-------+-----+-------+--------+
|     3 | C    |     3 |   1 |     3 | Paris  |
|     3 | E    |     3 |   1 |     3 | Paris  |
|     4 | F    |     4 |  11 |     4 | Berlin |
|     4 | F    |     4 |  22 |     4 | Berlin |
+-------+------+-------+-----+-------+--------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input3, ON('t1.id', 'id')),
            WHERE(r => r.age > 0 && r['t1.id'] > 0),
            ORDER_BY('t1.id', 't2.id', 't3.id'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`selected columns - 3 inputs`, () => {
            const table = `
+--------+-------+-----+-------+
| city   | t2.id | age | t3.id |
+--------+-------+-----+-------+
| Paris  |     3 |   1 |     3 |
| Paris  |     3 |   1 |     3 |
| Berlin |     4 |  11 |     4 |
| Berlin |     4 |  22 |     4 |
+--------+-------+-----+-------+
4 rows selected`;

            const res =
            SELECT('city', 't2.id', 'age', 't3.id',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input3, ON('t1.id', 'id')),
            WHERE(r => r.age > 0 && r['t1.id'] > 0),
            ORDER_BY('t1.id'))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - 4 inputs`, () => {
            const table = `
+-------+------+-------+-----+-------+---------+-------+---------+
| t1.id | name | t2.id | age | t3.id | t3.city | t4.id | t4.city |
+-------+------+-------+-----+-------+---------+-------+---------+
|     3 | C    |     3 |   1 |     3 | Paris   |     3 | Paris   |
|     3 | E    |     3 |   1 |     3 | Paris   |     3 | Paris   |
|     4 | F    |     4 |  11 |     4 | Berlin  |     4 | Berlin  |
|     4 | F    |     4 |  22 |     4 | Berlin  |     4 | Berlin  |
+-------+------+-------+-----+-------+---------+-------+---------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input3, ON('t1.id', 'id'),
                         JOIN, input3, ON('t1.id', 'id')),
            WHERE(r => r['t2.id'] > 0 && r.age > 0),
            ORDER_BY('t4.id'))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`selected columns - 4 inputs`, () => {
            const table = `
+---------+-------+-----+------+
| t4.city | t1.id | age | name |
+---------+-------+-----+------+
| Paris   |     3 |   1 | C    |
| Paris   |     3 |   1 | E    |
| Berlin  |     4 |  11 | F    |
| Berlin  |     4 |  22 | F    |
+---------+-------+-----+------+
4 rows selected`;

            const res =
            SELECT('t4.city', 't1.id', 'age', 'name',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input3, ON('t1.id', 'id'),
                         JOIN, input3, ON('t1.id', 'id')),
            WHERE(r => r['t1.id'] > 0 && r.age > 0),
            ORDER_BY('t4.city', DESC))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - 3 inputs - more join-columns`, () => {
            const table = `
+-------+------+-------+--------+-------+--------+
| t1.id | name | t2.id | t2.age | t3.id | t3.age |
+-------+------+-------+--------+-------+--------+
|     3 | C    |     3 |      1 |     3 |      1 |
|     3 | E    |     3 |      1 |     3 |      1 |
|     4 | F    |     4 |     11 |     4 |     11 |
|     4 | F    |     4 |     22 |     4 |     22 |
+-------+------+-------+--------+-------+--------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input2, ON('age', 'age')),
            WHERE(r => r['t1.id'] > 0 && r['t3.age'] > 0),
            ORDER_BY('t2.id'))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`selected columns - 3 inputs - more join-columns`, () => {
            const table = `
+------+-------+--------+
| name | t2.id | t3.age |
+------+-------+--------+
| C    |     3 |      1 |
| E    |     3 |      1 |
| F    |     4 |     11 |
| F    |     4 |     22 |
+------+-------+--------+
4 rows selected`;

            const res =
            SELECT('name', 't2.id', 't3.age',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input2, ON('age', 'age')),
            WHERE(r => r['t3.id'] > 0 && r['t3.age'] > 0),
            ORDER_BY('t1.id'))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - 4 inputs - more join-columns`, () => {
            const table = `
+-------+------+-------+-----+-------+---------+-------+---------+
| t1.id | name | t2.id | age | t3.id | t3.city | t4.id | t4.city |
+-------+------+-------+-----+-------+---------+-------+---------+
|     3 | C    |     3 |   1 |     3 | Paris   |     3 | Paris   |
|     3 | E    |     3 |   1 |     3 | Paris   |     3 | Paris   |
|     4 | F    |     4 |  11 |     4 | Berlin  |     4 | Berlin  |
|     4 | F    |     4 |  22 |     4 | Berlin  |     4 | Berlin  |
+-------+------+-------+-----+-------+---------+-------+---------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input3, ON('t1.id', 'id'),
                         JOIN, input3, ON('city', 'city')),
            WHERE(r => r['t3.id'] > 0 && r['age'] > 0),
            ORDER_BY('t1.id'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`selected columns - 4 inputs - more join-columns`, () => {
            const table = `
+---------+-------+-----+------+
| t4.city | t1.id | age | name |
+---------+-------+-----+------+
| Paris   |     3 |   1 | C    |
| Paris   |     3 |   1 | E    |
| Berlin  |     4 |  11 | F    |
| Berlin  |     4 |  22 | F    |
+---------+-------+-----+------+
4 rows selected`;

            const res =
            SELECT('t4.city', 't1.id', 'age', 'name',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input3, ON('t1.id', 'id'),
                         JOIN, input3, ON('city', 'city')),
            WHERE(r => r['t4.id'] > 0 && r['age'] > 0),
            ORDER_BY('t3.city', DESC))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - 5 inputs - more join-columns`, () => {
            const table = `
+-------+------+-------+--------+-------+--------+-------+---------+-------+---------+
| t1.id | name | t2.id | t2.age | t3.id | t3.age | t4.id | t4.city | t5.id | t5.city |
+-------+------+-------+--------+-------+--------+-------+---------+-------+---------+
|     3 | C    |     3 |      1 |     3 |      1 |     3 | Paris   |     3 | Paris   |
|     3 | E    |     3 |      1 |     3 |      1 |     3 | Paris   |     3 | Paris   |
|     4 | F    |     4 |     11 |     4 |     11 |     4 | Berlin  |     4 | Berlin  |
|     4 | F    |     4 |     22 |     4 |     22 |     4 | Berlin  |     4 | Berlin  |
+-------+------+-------+--------+-------+--------+-------+---------+-------+---------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input2, ON('age', 'age'),
                         JOIN, input3, ON('t1.id', 'id'),
                         JOIN, input3, ON('city', 'city')),
            WHERE(r => r['t4.id'] > 0 && r['name'] > 'A'),
            ORDER_BY('t5.city', DESC))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`selected columns - 5 inputs - more join-columns`, () => {
            const table = `
+------+-------+--------+---------+
| name | t2.id | t3.age | t5.city |
+------+-------+--------+---------+
| C    |     3 |      1 | Paris   |
| E    |     3 |      1 | Paris   |
| F    |     4 |     11 | Berlin  |
| F    |     4 |     22 | Berlin  |
+------+-------+--------+---------+
4 rows selected`;

            const res =
            SELECT('name', 't2.id', 't3.age', 't5.city',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input2, ON('age', 'age'),
                         JOIN, input3, ON('t1.id', 'id'),
                         JOIN, input3, ON('city', 'city')),
            WHERE(r => r['t4.id'] > 0 && r['t2.age'] > 0),
            ORDER_BY('t4.city', DESC, 'name'))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });


    });






    describe('with USING:', () => {
        it(`asterisk - 3 inputs`, () => {
            const table = `
+----+------+-----+--------+
| id | name | age | city   |
+----+------+-----+--------+
|  3 | C    |   1 | Paris  |
|  3 | E    |   1 | Paris  |
|  4 | F    |  11 | Berlin |
|  4 | F    |  22 | Berlin |
+----+------+-----+--------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, USING('id')),
            WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0 && r.age > 0 && r.id > 0),
            ORDER_BY('city', DESC, 'name'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`selected columns - 3 inputs`, () => {
            const table = `
+--------+----+-------+-----+-------+
| city   | id | t2.id | age | t3.id |
+--------+----+-------+-----+-------+
| Paris  |  3 |     3 |   1 |     3 |
| Paris  |  3 |     3 |   1 |     3 |
| Berlin |  4 |     4 |  11 |     4 |
| Berlin |  4 |     4 |  22 |     4 |
+--------+----+-------+-----+-------+
4 rows selected`;

            const res =
            SELECT('city', 'id', 't2.id', 'age', 't3.id',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, USING('id')),
            WHERE(r => r.age > 0 && r.id > 0),
            ORDER_BY('t1.id', 'name'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - 4 inputs`, () => {
            const table = `
+----+------+-----+---------+---------+
| id | name | age | t3.city | t4.city |
+----+------+-----+---------+---------+
|  3 | C    |   1 | Paris   | Paris   |
|  3 | E    |   1 | Paris   | Paris   |
|  4 | F    |  11 | Berlin  | Berlin  |
|  4 | F    |  22 | Berlin  | Berlin  |
+----+------+-----+---------+---------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, USING('id'),
                         JOIN, input3, USING('id')),
            WHERE(r => r.age > 0 && r.id > 0),
            ORDER_BY('t1.id', 't2.id', 't3.id', 't4.id'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`selected columns - 4 inputs`, () => {
            const table = `
+---------+----+-----+------+
| t4.city | id | age | name |
+---------+----+-----+------+
| Paris   |  3 |   1 | C    |
| Paris   |  3 |   1 | E    |
| Berlin  |  4 |  11 | F    |
| Berlin  |  4 |  22 | F    |
+---------+----+-----+------+
4 rows selected`;

            const res =
            SELECT('t4.city', 'id', 'age', 'name',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, USING('id'),
                         JOIN, input3, USING('id')),
            WHERE(r => r['t4.city'] > 'A' && r.id > 0),
            ORDER_BY('t1.id', 'id', 't4.id'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - 3 inputs - more join-columns`, () => {
            const table = `
+----+------+-----+-------+
| id | name | age | t3.id |
+----+------+-----+-------+
|  3 | C    |   1 |     3 |
|  3 | E    |   1 |     3 |
|  4 | F    |  11 |     4 |
|  4 | F    |  22 |     4 |
+----+------+-----+-------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input2, USING('age')),
            WHERE(r => r.id > 0 && r.age > 0),
            ORDER_BY('t1.id', 'id', 't3.id'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`selected columns - 3 inputs - more join-columns`, () => {
            const table = `
+------+-------+--------+
| name | t2.id | t3.age |
+------+-------+--------+
| C    |     3 |      1 |
| E    |     3 |      1 |
| F    |     4 |     11 |
| F    |     4 |     22 |
+------+-------+--------+
4 rows selected`;

            const res =
            SELECT('name', 't2.id', 't3.age',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input2, USING('age')),
            WHERE(r => r.id > 0 && r.age > 0),
            ORDER_BY('t1.id', 'id', 't3.id'))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - 4 inputs - more join-columns`, () => {
            const table = `
+----+------+-----+--------+-------+
| id | name | age | city   | t4.id |
+----+------+-----+--------+-------+
|  3 | C    |   1 | Paris  |     3 |
|  3 | E    |   1 | Paris  |     3 |
|  4 | F    |  11 | Berlin |     4 |
|  4 | F    |  22 | Berlin |     4 |
+----+------+-----+--------+-------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, USING('id'),
                         JOIN, input3, USING('city')),
            WHERE(r => r['t2.id'] > 0 && r.age > 0),
            ORDER_BY('t1.id', 'id', 't3.id'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`selected columns - 4 inputs - more join-columns`, () => {
            const table = `
+---------+-------+-----+------+
| t4.city | t1.id | age | name |
+---------+-------+-----+------+
| Paris   |     3 |   1 | C    |
| Paris   |     3 |   1 | E    |
| Berlin  |     4 |  11 | F    |
| Berlin  |     4 |  22 | F    |
+---------+-------+-----+------+
4 rows selected`;

            const res =
            SELECT('t4.city', 't1.id', 'age', 'name',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, USING('id'),
                         JOIN, input3, USING('city')),
            WHERE(r => r.city > 'A' && r.age > 0),
            ORDER_BY('t1.id', 'id', 't3.id', 'city', DESC))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - 5 inputs - more join-columns`, () => {
            const table = `
+----+------+-----+-------+--------+-------+
| id | name | age | t3.id | city   | t5.id |
+----+------+-----+-------+--------+-------+
|  3 | C    |   1 |     3 | Paris  |     3 |
|  3 | E    |   1 |     3 | Paris  |     3 |
|  4 | F    |  11 |     4 | Berlin |     4 |
|  4 | F    |  22 |     4 | Berlin |     4 |
+----+------+-----+-------+--------+-------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input2, USING('age'),
                         JOIN, input3, USING('id'),
                         JOIN, input3, USING('city')),
            WHERE(r => r.city > 'A' && r['t4.city'] > 'A' && r['t5.city'] > 'A'),
            ORDER_BY('age', 't2.age', 't3.age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`selected columns - 5 inputs - more join-columns`, () => {
            const table = `
+------+-------+--------+---------+
| name | t2.id | t3.age | t5.city |
+------+-------+--------+---------+
| C    |     3 |      1 | Paris   |
| E    |     3 |      1 | Paris   |
| F    |     4 |     11 | Berlin  |
| F    |     4 |     22 | Berlin  |
+------+-------+--------+---------+
4 rows selected`;

            const res =
            SELECT('name', 't2.id', 't3.age', 't5.city',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input2, USING('age'),
                         JOIN, input3, USING('id'),
                         JOIN, input3, USING('city')),
            WHERE(r => r.city > 'A' && r['t4.city'] > 'A' && r['t5.city'] > 'A'),
            ORDER_BY('age', 't2.age', 't3.age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });


    });





    describe('mixed (ON/USING):', () => {
        it(`asterisk - 3 inputs - 1`, () => {
            const table = `
+----+------+-----+-------+--------+
| id | name | age | t3.id | city   |
+----+------+-----+-------+--------+
|  3 | C    |   1 |     3 | Paris  |
|  3 | E    |   1 |     3 | Paris  |
|  4 | F    |  11 |     4 | Berlin |
|  4 | F    |  22 |     4 | Berlin |
+----+------+-----+-------+--------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, ON('t1.id', 'id')),
            WHERE(r => r.id > 0 && r['t1.id'] > 0 && r['t2.id'] > 0),
            ORDER_BY('age', 'id'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - 3 inputs - 2`, () => {
            const table = `
+----+------+-----+-------+--------+
| id | name | age | t3.id | city   |
+----+------+-----+-------+--------+
|  3 | C    |   1 |     3 | Paris  |
|  3 | E    |   1 |     3 | Paris  |
|  4 | F    |  11 |     4 | Berlin |
|  4 | F    |  22 |     4 | Berlin |
+----+------+-----+-------+--------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, ON('t2.id', 'id')),
            WHERE(r => r.id > 0 && r['t1.id'] > 0 && r['t2.id'] > 0),
            ORDER_BY('age', 'id'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - 3 inputs (ON/USING)`, () => {
            const table = `
+-------+------+-------+-----+-------+
| t1.id | name | t2.id | age | t3.id |
+-------+------+-------+-----+-------+
|     3 | C    |     3 |   1 |     3 |
|     3 | E    |     3 |   1 |     3 |
|     4 | F    |     4 |  11 |     4 |
|     4 | F    |     4 |  22 |     4 |
+-------+------+-------+-----+-------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input2, USING('age')),
            WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0),
            ORDER_BY('age', 't2.age', 't3.age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`selected columns - 3 inputs`, () => {
            const table = `
+----+--------+-------+-----+-------+
| id | city   | t2.id | age | t3.id |
+----+--------+-------+-----+-------+
|  3 | Paris  |     3 |   1 |     3 |
|  3 | Paris  |     3 |   1 |     3 |
|  4 | Berlin |     4 |  11 |     4 |
|  4 | Berlin |     4 |  22 |     4 |
+----+--------+-------+-----+-------+
4 rows selected`;

            const res =
            SELECT('id', 'city', 't2.id', 'age', 't3.id',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, ON('t1.id', 'id')),
            WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0),
            ORDER_BY('id', 'age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - 4 inputs - 1 USING`, () => {
            const table = `
+----+------+-----+-------+---------+-------+---------+
| id | name | age | t3.id | t3.city | t4.id | t4.city |
+----+------+-----+-------+---------+-------+---------+
|  3 | C    |   1 |     3 | Paris   |     3 | Paris   |
|  3 | E    |   1 |     3 | Paris   |     3 | Paris   |
|  4 | F    |  11 |     4 | Berlin  |     4 | Berlin  |
|  4 | F    |  22 |     4 | Berlin  |     4 | Berlin  |
+----+------+-----+-------+---------+-------+---------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, ON('t2.id', 'id'),
                         JOIN, input3, ON('t1.id', 'id')),
            WHERE(r => r['t4.id'] > 0 && r['t3.id'] > 0),
            ORDER_BY('id', 'age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`selected columns - 4 inputs - 1 USING`, () => {
            const table = `
+------+-----+---------+
| name | age | t4.city |
+------+-----+---------+
| C    |   1 | Paris   |
| E    |   1 | Paris   |
| F    |  11 | Berlin  |
| F    |  22 | Berlin  |
+------+-----+---------+
4 rows selected`;

            const res =
            SELECT('name', 'age', 't4.city',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, ON('t1.id', 'id'),
                         JOIN, input3, ON('t1.id', 'id')),
            WHERE(r => r['t1.id'] > 0 && r['t4.id'] > 0),
            ORDER_BY('id', 'age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk -  4 inputs - 2 USING`, () => {
            const table = `
+----+------+-----+---------+-------+---------+
| id | name | age | t3.city | t4.id | t4.city |
+----+------+-----+---------+-------+---------+
|  3 | C    |   1 | Paris   |     3 | Paris   |
|  3 | E    |   1 | Paris   |     3 | Paris   |
|  4 | F    |  11 | Berlin  |     4 | Berlin  |
|  4 | F    |  22 | Berlin  |     4 | Berlin  |
+----+------+-----+---------+-------+---------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, USING('id'),
                         JOIN, input3, ON('t1.id', 'id')),
            WHERE(r => r.id > 0 && r['t2.id'] > 0),
            ORDER_BY('id', 'age', 't3.city', DESC))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk -  4 inputs - 2 USING`, () => {
            const table = `
+------+----+-------+---------+
| name | id | t1.id | t4.city |
+------+----+-------+---------+
| C    |  3 |     3 | Paris   |
| E    |  3 |     3 | Paris   |
| F    |  4 |     4 | Berlin  |
| F    |  4 |     4 | Berlin  |
+------+----+-------+---------+
4 rows selected`;

            const res =
            SELECT('name', 'id', 't1.id', 't4.city',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, USING('id'),
                         JOIN, input3, ON('t1.id', 'id')),
            WHERE(r => r.id > 0 && r['t4.id'] > 0),
            ORDER_BY('age', 't3.city', DESC, 't4.city', DESC))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - 4 inputs - (USING/ON/USING)`, () => {
            const table = `
+----+------+-----+-------+--------+-------+
| id | name | age | t3.id | city   | t4.id |
+----+------+-----+-------+--------+-------+
|  3 | C    |   1 |     3 | Paris  |     3 |
|  3 | E    |   1 |     3 | Paris  |     3 |
|  4 | F    |  11 |     4 | Berlin |     4 |
|  4 | F    |  22 |     4 | Berlin |     4 |
+----+------+-----+-------+--------+-------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, ON('t1.id', 'id'),
                         JOIN, input3, USING('city')),
            WHERE(r => r.id > 0 && r['t2.id'] > 0),
            ORDER_BY('id', 'age', 't3.city', DESC))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });


    });

    describe('crossjoin:', () => {        
        it(`asterisk - 3 inputs`, () => {            
            const table = `
+-------+------+-------+-----+-------+--------+
| t1.id | name | t2.id | age | t3.id | city   |
+-------+------+-------+-----+-------+--------+
|     1 | A    |     3 |   1 |     2 | London |
|     1 | A    |     3 |   1 |     3 | Paris  |
|     1 | A    |     4 |  11 |     2 | London |
|     1 | A    |     4 |  11 |     3 | Paris  |
|     2 | B    |     3 |   1 |     2 | London |
|     2 | B    |     3 |   1 |     3 | Paris  |
|     2 | B    |     4 |  11 |     2 | London |
|     2 | B    |     4 |  11 |     3 | Paris  |
+-------+------+-------+-----+-------+--------+
8 rows selected`;

            const res1 =
            SELECT('*',
            FROM(input11, CROSS_JOIN, input22,
                          CROSS_JOIN, input33),
            WHERE(r => r['t2.id'] > 0),
            ORDER_BY('t1.id', 'age'))
              

            const res2 =
            SELECT('*',
            FROM(input11, input22, input33),
            WHERE(r => r['t3.id'] > 0),
            ORDER_BY('name', 'age'))

            expect(trim(drawTable(res1))).toEqual(trim(table));
            expect(trim(drawTable(res2))).toEqual(trim(table));
        });

        it(`asterisk - mixed joins (JOIN(USING)/CROSS_JOIN) - 3 inputs`, () => {
            const table = `
+----+---------+-----+-------+---------+
| id | t1.name | age | t3.id | t3.name |
+----+---------+-----+-------+---------+
|  3 | C       |   1 |     1 | A       |
|  3 | C       |   1 |     2 | B       |
|  3 | E       |   1 |     1 | A       |
|  3 | E       |   1 |     2 | B       |
|  4 | F       |  11 |     1 | A       |
|  4 | F       |  11 |     2 | B       |
|  4 | F       |  22 |     1 | A       |
|  4 | F       |  22 |     2 | B       |
+----+---------+-----+-------+---------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         CROSS_JOIN, input11),
            WHERE(r => r.id > 0 && r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
            ORDER_BY('t1.id', 'age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - mixed joins (JOIN(ON)/CROSS_JOIN) - 3 inputs`, () => {
            const table = `
+-------+---------+-------+-----+-------+---------+
| t1.id | t1.name | t2.id | age | t3.id | t3.name |
+-------+---------+-------+-----+-------+---------+
|     3 | C       |     3 |   1 |     1 | A       |
|     3 | C       |     3 |   1 |     2 | B       |
|     3 | E       |     3 |   1 |     1 | A       |
|     3 | E       |     3 |   1 |     2 | B       |
|     4 | F       |     4 |  11 |     1 | A       |
|     4 | F       |     4 |  11 |     2 | B       |
|     4 | F       |     4 |  22 |     1 | A       |
|     4 | F       |     4 |  22 |     2 | B       |
+-------+---------+-------+-----+-------+---------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         CROSS_JOIN, input11),
            WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
            ORDER_BY('t1.name', 'age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk -  mixed joins (JOIN(USING/ON)/CROSS_JOIN)`, () => {
            const table = `
+----+------+-----+---------+-------+---------+-------+---------+
| id | name | age | t3.city | t4.id | t4.city | t5.id | t5.city |
+----+------+-----+---------+-------+---------+-------+---------+
|  3 | C    |   1 | Paris   |     3 | Paris   |     2 | London  |
|  3 | C    |   1 | Paris   |     3 | Paris   |     3 | Paris   |
|  3 | E    |   1 | Paris   |     3 | Paris   |     2 | London  |
|  3 | E    |   1 | Paris   |     3 | Paris   |     3 | Paris   |
|  4 | F    |  11 | Berlin  |     4 | Berlin  |     2 | London  |
|  4 | F    |  11 | Berlin  |     4 | Berlin  |     3 | Paris   |
|  4 | F    |  22 | Berlin  |     4 | Berlin  |     2 | London  |
|  4 | F    |  22 | Berlin  |     4 | Berlin  |     3 | Paris   |
+----+------+-----+---------+-------+---------+-------+---------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         JOIN, input3, USING('id'),
                         JOIN, input3, ON('t1.id', 'id'),
                         CROSS_JOIN, input33),
            WHERE(r => r['id'] > 0 && r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] && r['t4.id'] && r['t5.id']),
            ORDER_BY('t1.id', 'age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - mixed joins (CROSS_JOIN/JOIN(ON))`, () => {            
            const table = `
+-------+------+-------+-----+-------+---------+-------+---------+
| t1.id | name | t2.id | age | t3.id | t3.city | t4.id | t4.city |
+-------+------+-------+-----+-------+---------+-------+---------+
|     1 | A    |     3 |   1 |     2 | London  |     3 | Paris   |
|     1 | A    |     3 |   1 |     3 | Paris   |     3 | Paris   |
|     2 | B    |     3 |   1 |     2 | London  |     3 | Paris   |
|     2 | B    |     3 |   1 |     3 | Paris   |     3 | Paris   |
+-------+------+-------+-----+-------+---------+-------+---------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input11, CROSS_JOIN, input22,
                          CROSS_JOIN, input33,
                          JOIN, input33, ON('t2.id', 'id')),
            WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
            ORDER_BY('name', 'age'))
              
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - mixed joins (CROSS_JOIN/JOIN(USING))`, () => {            
            const table = `
+-------+------+-------+-----+-------+--------+-------+
| t1.id | name | t2.id | age | t3.id | city   | t4.id |
+-------+------+-------+-----+-------+--------+-------+
|     1 | A    |     3 |   1 |     2 | London |     3 |
|     1 | A    |     3 |   1 |     3 | Paris  |     3 |
|     1 | A    |     4 |  11 |     2 | London |     4 |
|     1 | A    |     4 |  11 |     3 | Paris  |     4 |
|     2 | B    |     3 |   1 |     2 | London |     3 |
|     2 | B    |     3 |   1 |     3 | Paris  |     3 |
|     2 | B    |     4 |  11 |     2 | London |     4 |
|     2 | B    |     4 |  11 |     3 | Paris  |     4 |
+-------+------+-------+-----+-------+--------+-------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input11, CROSS_JOIN, input22,
                          CROSS_JOIN, input33,
                          JOIN, input22, USING('age')),
            WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
            ORDER_BY('name', 'age'))
              
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - mixed joins (CROSS_JOIN/JOIN(USING/ON))`, () => {            
            const table = `
+-------+------+-------+-----+-------+---------+-------+-------+---------+
| t1.id | name | t2.id | age | t3.id | t3.city | t4.id | t5.id | t5.city |
+-------+------+-------+-----+-------+---------+-------+-------+---------+
|     1 | A    |     3 |   1 |     2 | London  |     3 |     2 | London  |
|     1 | A    |     3 |   1 |     3 | Paris   |     3 |     3 | Paris   |
|     1 | A    |     4 |  11 |     2 | London  |     4 |     2 | London  |
|     1 | A    |     4 |  11 |     3 | Paris   |     4 |     3 | Paris   |
|     2 | B    |     3 |   1 |     2 | London  |     3 |     2 | London  |
|     2 | B    |     3 |   1 |     3 | Paris   |     3 |     3 | Paris   |
|     2 | B    |     4 |  11 |     2 | London  |     4 |     2 | London  |
|     2 | B    |     4 |  11 |     3 | Paris   |     4 |     3 | Paris   |
+-------+------+-------+-----+-------+---------+-------+-------+---------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input11, CROSS_JOIN, input22,
                          CROSS_JOIN, input33,
                          JOIN, input22, USING('age'),
                          JOIN, input33, ON('city', 'city')),
            WHERE(r => r['t1.id'] > 0 && r['t3.city'] > 'A' && r['t5.city'] > 'A' && r['t4.age'] > 0),
            ORDER_BY('name', 'age'))
              
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - mixed joins (JOIN(USING)/CROSS_JOIN/JOIN(USING)) - 3 inputs`, () => {
            const table = `
+----+---------+-----+-------+---------+-------+
| id | t1.name | age | t3.id | t3.name | t4.id |
+----+---------+-----+-------+---------+-------+
|  3 | C       |   1 |     1 | A       |     3 |
|  3 | C       |   1 |     2 | B       |     3 |
|  3 | E       |   1 |     1 | A       |     3 |
|  3 | E       |   1 |     2 | B       |     3 |
|  4 | F       |  11 |     1 | A       |     4 |
|  4 | F       |  11 |     2 | B       |     4 |
|  4 | F       |  22 |     1 | A       |     4 |
|  4 | F       |  22 |     2 | B       |     4 |
+----+---------+-----+-------+---------+-------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         CROSS_JOIN, input11,
                         JOIN, input2, USING('age')),
            WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
            ORDER_BY('t1.name', 'age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - mixed joins (JOIN(ON)/CROSS_JOIN/JOIN(ON)) - 3 inputs`, () => {
            const table = `
+-------+---------+-------+--------+-------+---------+-------+--------+
| t1.id | t1.name | t2.id | t2.age | t3.id | t3.name | t4.id | t4.age |
+-------+---------+-------+--------+-------+---------+-------+--------+
|     3 | C       |     3 |      1 |     1 | A       |     3 |      1 |
|     3 | C       |     3 |      1 |     2 | B       |     3 |      1 |
|     3 | E       |     3 |      1 |     1 | A       |     3 |      1 |
|     3 | E       |     3 |      1 |     2 | B       |     3 |      1 |
|     4 | F       |     4 |     11 |     1 | A       |     4 |     11 |
|     4 | F       |     4 |     11 |     2 | B       |     4 |     11 |
|     4 | F       |     4 |     22 |     1 | A       |     4 |     22 |
|     4 | F       |     4 |     22 |     2 | B       |     4 |     22 |
+-------+---------+-------+--------+-------+---------+-------+--------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         CROSS_JOIN, input11,
                         JOIN, input2, ON('age', 'age')),
            WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
            ORDER_BY('t1.name', 't4.age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - mixed joins (JOIN(USING)/CROSS_JOIN/JOIN(ON)) - 3 inputs`, () => {
            const table = `
+----+---------+--------+-------+---------+-------+--------+
| id | t1.name | t2.age | t3.id | t3.name | t4.id | t4.age |
+----+---------+--------+-------+---------+-------+--------+
|  3 | C       |      1 |     1 | A       |     3 |      1 |
|  3 | C       |      1 |     2 | B       |     3 |      1 |
|  3 | E       |      1 |     1 | A       |     3 |      1 |
|  3 | E       |      1 |     2 | B       |     3 |      1 |
|  4 | F       |     11 |     1 | A       |     4 |     11 |
|  4 | F       |     11 |     2 | B       |     4 |     11 |
|  4 | F       |     22 |     1 | A       |     4 |     22 |
|  4 | F       |     22 |     2 | B       |     4 |     22 |
+----+---------+--------+-------+---------+-------+--------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         CROSS_JOIN, input11,
                         JOIN, input2, ON('age', 'age')),
            WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
            ORDER_BY('t1.name', 't4.age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`asterisk - mixed joins (JOIN(ON)/CROSS_JOIN/JOIN(USING)) - 3 inputs`, () => {
            const table = `
+-------+---------+-------+-----+-------+---------+-------+
| t1.id | t1.name | t2.id | age | t3.id | t3.name | t4.id |
+-------+---------+-------+-----+-------+---------+-------+
|     3 | C       |     3 |   1 |     1 | A       |     3 |
|     3 | C       |     3 |   1 |     2 | B       |     3 |
|     3 | E       |     3 |   1 |     1 | A       |     3 |
|     3 | E       |     3 |   1 |     2 | B       |     3 |
|     4 | F       |     4 |  11 |     1 | A       |     4 |
|     4 | F       |     4 |  11 |     2 | B       |     4 |
|     4 | F       |     4 |  22 |     1 | A       |     4 |
|     4 | F       |     4 |  22 |     2 | B       |     4 |
+-------+---------+-------+-----+-------+---------+-------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         CROSS_JOIN, input11,
                         JOIN, input2, USING('age')),
            WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
            ORDER_BY('t1.name', 't4.age'))
             
            expect(trim(drawTable(res))).toEqual(trim(table));
        });








    });





});