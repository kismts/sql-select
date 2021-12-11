import { SELECT, JOIN, FROM, ON, USING, LEFT_SEMI_JOIN, RIGHT_SEMI_JOIN, LEFT_ANTI_JOIN,
         RIGHT_ANTI_JOIN, CROSS_JOIN } from '../../dist/select.js'
import { input1, input2, input3, input11, input22, input33 } from './data.js'
import { drawTable } from './utils.js'
const trim = str => str.trim();




describe('joins_3 - semijoins:', () => {
    describe('leftsemijoin: ', () => {
        it(`1`, () => {
            const table = `
+----+------+
| id | name |
+----+------+
|  3 | C    |
|  3 | E    |
|  4 | F    |
+----+------+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_SEMI_JOIN, input2, ON('id', 'id'),
                         LEFT_SEMI_JOIN, input3, ON('id', 'id'),
                         LEFT_SEMI_JOIN, input3, ON('id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`2`, () => {
            const table = `
+----+------+
| id | name |
+----+------+
|  3 | C    |
|  3 | E    |
|  4 | F    |
+----+------+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_SEMI_JOIN, input2, USING('id'),
                         LEFT_SEMI_JOIN, input3, USING('id'),
                         LEFT_SEMI_JOIN, input3, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`3`, () => {
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
                         JOIN, input3, ON('t1.id', 'id'),
                         LEFT_SEMI_JOIN, input2, ON('t1.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`4`, () => {
            const table = `
+----+------+-----+
| id | name | age |
+----+------+-----+
|  3 | C    |   1 |
|  3 | E    |   1 |
|  4 | F    |  11 |
|  4 | F    |  22 |
+----+------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         LEFT_SEMI_JOIN, input2, ON('t1.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`5`, () => {
            const table = `
+-------+------+-------+-----+
| t1.id | name | t2.id | age |
+-------+------+-------+-----+
|     3 | C    |     3 |   1 |
|     3 | E    |     3 |   1 |
|     4 | F    |     4 |  11 |
|     4 | F    |     4 |  22 |
+-------+------+-------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         LEFT_SEMI_JOIN, input2, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });


        it(`6`, () => {
            const table = `
+----+------+-----+
| id | name | age |
+----+------+-----+
|  3 | C    |   1 |
|  3 | E    |   1 |
|  4 | F    |  11 |
|  4 | F    |  22 |
+----+------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         LEFT_SEMI_JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`7`, () => {
            const table = `
+-------+------+-------+--------+-------+-----+
| t1.id | name | t3.id | city   | t4.id | age |
+-------+------+-------+--------+-------+-----+
|     3 | C    |     3 | Paris  |     3 |   1 |
|     3 | E    |     3 | Paris  |     3 |   1 |
|     4 | F    |     4 | Berlin |     4 |  11 |
|     4 | F    |     4 | Berlin |     4 |  22 |
+-------+------+-------+--------+-------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_SEMI_JOIN, input2, ON('id', 'id'),
                         JOIN, input3, ON('id', 'id'),
                         JOIN, input2, ON('t1.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`8`, () => {
            const table = `
+-------+------+-------+-----+
| t1.id | name | t3.id | age |
+-------+------+-------+-----+
|     3 | C    |     3 |   1 |
|     3 | E    |     3 |   1 |
|     4 | F    |     4 |  11 |
|     4 | F    |     4 |  22 |
+-------+------+-------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_SEMI_JOIN, input2, ON('id', 'id'),
                         JOIN, input2, ON('id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`9`, () => {
            const table = `
+-------+------+-------+
| t1.id | name | t3.id |
+-------+------+-------+
|     3 | C    |     3 |
|     3 | E    |     3 |
|     4 | F    |     4 |
+-------+------+-------+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_SEMI_JOIN, input2, USING('id'),
                         JOIN, input1, USING('name')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });


        it(`10`, () => {
            const table = `
+----+------+-----+
| id | name | age |
+----+------+-----+
|  3 | C    |   1 |
|  3 | E    |   1 |
|  4 | F    |  11 |
|  4 | F    |  22 |
+----+------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_SEMI_JOIN, input2, USING('id'),
                         JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`11`, () => {
            const table = `
+-------+------+-------+-----+
| t1.id | name | t2.id | age |
+-------+------+-------+-----+
|     1 | A    |     3 |   1 |
|     1 | A    |     4 |  11 |
|     2 | B    |     3 |   1 |
|     2 | B    |     4 |  11 |
+-------+------+-------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input11, CROSS_JOIN, input22,
                          LEFT_SEMI_JOIN, input2, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`12`, () => {
            const table = `
+-------+------+-------+-----+
| t1.id | name | t3.id | age |
+-------+------+-------+-----+
|     3 | C    |     3 |   1 |
|     3 | C    |     4 |  11 |
|     3 | E    |     3 |   1 |
|     3 | E    |     4 |  11 |
|     4 | F    |     3 |   1 |
|     4 | F    |     4 |  11 |
+-------+------+-------+-----+
6 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_SEMI_JOIN, input2, USING('id'),
                         CROSS_JOIN, input22))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });



        it(`13`, () => {
            const table = `
+----+------+--------+--------+
| id | name | t2.age | t4.age |
+----+------+--------+--------+
|  3 | C    |      1 |      1 |
|  3 | E    |      1 |      1 |
|  4 | F    |     11 |     11 |
|  4 | F    |     22 |     11 |
|  4 | F    |     11 |     22 |
|  4 | F    |     22 |     22 |
+----+------+--------+--------+
6 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         LEFT_SEMI_JOIN, input2, ON('t1.id', 'id'),
                         JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`14`, () => {
            const table = `
+-------+------+-------+--------+-------+--------+
| t1.id | name | t2.id | t2.age | t4.id | t4.age |
+-------+------+-------+--------+-------+--------+
|     3 | C    |     3 |      1 |     3 |      1 |
|     3 | E    |     3 |      1 |     3 |      1 |
|     4 | F    |     4 |     11 |     4 |     11 |
|     4 | F    |     4 |     22 |     4 |     11 |
|     4 | F    |     4 |     11 |     4 |     22 |
|     4 | F    |     4 |     22 |     4 |     22 |
+-------+------+-------+--------+-------+--------+
6 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         LEFT_SEMI_JOIN, input2, ON('t1.id', 'id'),
                         JOIN, input2, ON('t2.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`15`, () => {
            const table = `
+-------+------+-------+-----+-------+
| t1.id | name | t2.id | age | t4.id |
+-------+------+-------+-----+-------+
|     1 | A    |     3 |   1 |     3 |
|     2 | B    |     3 |   1 |     3 |
|     1 | A    |     4 |  11 |     4 |
|     2 | B    |     4 |  11 |     4 |
+-------+------+-------+-----+-------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input11, CROSS_JOIN, input22,
                          LEFT_SEMI_JOIN, input2, ON('t2.id', 'id'),
                          JOIN, input2, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`16`, () => {
            const table = `
+-------+------+-------+--------+-------+--------+
| t1.id | name | t2.id | t2.age | t4.id | t4.age |
+-------+------+-------+--------+-------+--------+
|     3 | C    |     3 |      1 |     3 |      1 |
|     3 | C    |     3 |      1 |     4 |     11 |
|     3 | E    |     3 |      1 |     3 |      1 |
|     3 | E    |     3 |      1 |     4 |     11 |
|     4 | F    |     4 |     11 |     3 |      1 |
|     4 | F    |     4 |     11 |     4 |     11 |
|     4 | F    |     4 |     22 |     3 |      1 |
|     4 | F    |     4 |     22 |     4 |     11 |
+-------+------+-------+--------+-------+--------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         LEFT_SEMI_JOIN, input2, ON('t1.id', 'id'),
                         CROSS_JOIN, input22))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`17`, () => {
            const table = `
+-------+------+-------+-----+
| t1.id | name | t3.id | age |
+-------+------+-------+-----+
|     3 | C    |     3 |   1 |
|     3 | E    |     3 |   1 |
|     4 | F    |     4 |  11 |
|     4 | F    |     4 |  22 |
+-------+------+-------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_SEMI_JOIN, input2, USING('id'),
                         JOIN, input2, ON('id', 'id'),
                         LEFT_SEMI_JOIN, input2, ON('t1.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`18`, () => {
            const table = `
+----+------+-----+
| id | name | age |
+----+------+-----+
|  3 | C    |   1 |
|  3 | E    |   1 |
|  4 | F    |  11 |
|  4 | F    |  22 |
+----+------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_SEMI_JOIN, input2, USING('id'),
                         JOIN, input2, USING('id'),
                         LEFT_SEMI_JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });
    });









    describe('leftantijoin: ', () => {
        it(`1`, () => {
            const table = `
+------+------+
|   id | name |
+------+------+
|    1 | A    |
| null | D    |
| null | G    |
+------+------+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_ANTI_JOIN, input2, ON('id', 'id'),
                         LEFT_ANTI_JOIN, input3, ON('id', 'id'),
                         LEFT_ANTI_JOIN, input3, ON('id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`2`, () => {
            const table = `
+------+------+
|   id | name |
+------+------+
|    1 | A    |
| null | D    |
| null | G    |
+------+------+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_ANTI_JOIN, input2, USING('id'),
                         LEFT_ANTI_JOIN, input3, USING('id'),
                         LEFT_ANTI_JOIN, input3, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`3`, () => {
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
                         JOIN, input3, ON('t1.id', 'id'),
                         LEFT_ANTI_JOIN, input11, ON('t1.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`4`, () => {
            const table = `
+----+------+-----+
| id | name | age |
+----+------+-----+
|  4 | F    |  11 |
|  4 | F    |  22 |
+----+------+-----+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         LEFT_ANTI_JOIN, input33, ON('t1.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`5`, () => {
            const table = `
+-------+------+-------+--------+
| t1.id | name | t2.id | city   |
+-------+------+-------+--------+
|     4 | F    |     4 | Berlin |
+-------+------+-------+--------+
1 row selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input3, ON('id', 'id'),
                         LEFT_ANTI_JOIN, input33, USING('city')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });


        it(`6`, () => {
            const table = `
+----+------+-----+
| id | name | age |
+----+------+-----+
|  4 | F    |  11 |
|  4 | F    |  22 |
+----+------+-----+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         LEFT_ANTI_JOIN, input33, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`7`, () => {
            const table = `
+-------+------+-------+--------+
| t1.id | name | t3.id | city   |
+-------+------+-------+--------+
|     2 | B    |     2 | London |
+-------+------+-------+--------+
1 row selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_ANTI_JOIN, input2, ON('id', 'id'),
                         JOIN, input3, ON('id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`8`, () => {
            const table = `
+--+
|  |
+--+
+--+
0 row selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_ANTI_JOIN, input2, ON('id', 'id'),
                         JOIN, input2, ON('id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`9`, () => {
            const table = `
+-------+------+-------+
| t1.id | name | t3.id |
+-------+------+-------+
|     1 | A    |     1 |
|     2 | B    |     2 |
|  null | D    |  null |
|  null | G    |  null |
+-------+------+-------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_ANTI_JOIN, input2, USING('id'),
                         JOIN, input1, USING('name')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });


        it(`10`, () => {
            const table = `
+----+---------+---------+
| id | t1.name | t3.name |
+----+---------+---------+
|  1 | A       | A       |
|  2 | B       | B       |
+----+---------+---------+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_ANTI_JOIN, input2, USING('id'),
                         JOIN, input11, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`11`, () => {
            const table = `
+--+
|  |
+--+
+--+
0 row selected`;

            const res =
            SELECT('*',
            FROM(input11, CROSS_JOIN, input22,
                          LEFT_ANTI_JOIN, input22, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`12`, () => {
            const table = `
+-------+------+-------+-----+
| t1.id | name | t3.id | age |
+-------+------+-------+-----+
|     1 | A    |     3 |   1 |
|     1 | A    |     4 |  11 |
|     2 | B    |     3 |   1 |
|     2 | B    |     4 |  11 |
|  null | D    |     3 |   1 |
|  null | D    |     4 |  11 |
|  null | G    |     3 |   1 |
|  null | G    |     4 |  11 |
+-------+------+-------+-----+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_ANTI_JOIN, input2, USING('id'),
                         CROSS_JOIN, input22))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });



        it(`13`, () => {
            const table = `
+----+------+--------+--------+
| id | name | t2.age | t4.age |
+----+------+--------+--------+
|  3 | C    |      1 |      1 |
|  3 | E    |      1 |      1 |
|  4 | F    |     11 |     11 |
|  4 | F    |     22 |     11 |
|  4 | F    |     11 |     22 |
|  4 | F    |     22 |     22 |
+----+------+--------+--------+
6 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         LEFT_ANTI_JOIN, input11, ON('t1.id', 'id'),
                         JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`14`, () => {
            const table = `
+-------+------+-------+--------+-------+--------+
| t1.id | name | t2.id | t2.age | t4.id | t4.age |
+-------+------+-------+--------+-------+--------+
|     3 | C    |     3 |      1 |     3 |      1 |
|     3 | E    |     3 |      1 |     3 |      1 |
|     4 | F    |     4 |     11 |     4 |     11 |
|     4 | F    |     4 |     22 |     4 |     11 |
|     4 | F    |     4 |     11 |     4 |     22 |
|     4 | F    |     4 |     22 |     4 |     22 |
+-------+------+-------+--------+-------+--------+
6 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         LEFT_ANTI_JOIN, input11, ON('t1.id', 'id'),
                         JOIN, input2, ON('t2.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`15`, () => {
            const table = `
+-------+------+-------+-----+-------+
| t1.id | name | t2.id | age | t4.id |
+-------+------+-------+-----+-------+
|     1 | A    |     3 |   1 |     3 |
|     2 | B    |     3 |   1 |     3 |
|     1 | A    |     4 |  11 |     4 |
|     2 | B    |     4 |  11 |     4 |
+-------+------+-------+-----+-------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input11, CROSS_JOIN, input22,
                          LEFT_ANTI_JOIN, input11, ON('t2.id', 'id'),
                          JOIN, input2, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`16`, () => {
            const table = `
+-------+------+-------+--------+-------+--------+
| t1.id | name | t2.id | t2.age | t4.id | t4.age |
+-------+------+-------+--------+-------+--------+
|     3 | C    |     3 |      1 |     3 |      1 |
|     3 | C    |     3 |      1 |     4 |     11 |
|     3 | E    |     3 |      1 |     3 |      1 |
|     3 | E    |     3 |      1 |     4 |     11 |
|     4 | F    |     4 |     11 |     3 |      1 |
|     4 | F    |     4 |     11 |     4 |     11 |
|     4 | F    |     4 |     22 |     3 |      1 |
|     4 | F    |     4 |     22 |     4 |     11 |
+-------+------+-------+--------+-------+--------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         LEFT_ANTI_JOIN, input11, ON('t1.id', 'id'),
                         CROSS_JOIN, input22))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`17`, () => {
            const table = `
+-------+---------+-------+---------+
| t1.id | t1.name | t3.id | t3.name |
+-------+---------+-------+---------+
|     1 | A       |     1 | A       |
|     2 | B       |     2 | B       |
+-------+---------+-------+---------+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_ANTI_JOIN, input2, USING('id'),
                         JOIN, input11, ON('id', 'id'),
                         LEFT_ANTI_JOIN, input2, ON('t1.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`18`, () => {
            const table = `
+----+---------+---------+
| id | t1.name | t3.name |
+----+---------+---------+
|  1 | A       | A       |
|  2 | B       | B       |
+----+---------+---------+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, LEFT_ANTI_JOIN, input2, USING('id'),
                         JOIN, input11, USING('id'),
                         LEFT_ANTI_JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });
    });









    describe('rightsemijoin: ', () => {
        it(`1`, () => {
            const table = `
+----+--------+
| id | city   |
+----+--------+
|  3 | Paris  |
|  4 | Berlin |
+----+--------+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_SEMI_JOIN, input2, ON('id', 'id'),
                         RIGHT_SEMI_JOIN, input3, ON('id', 'id'),
                         RIGHT_SEMI_JOIN, input3, ON('id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`2`, () => {
            const table = `
+----+--------+
| id | city   |
+----+--------+
|  3 | Paris  |
|  4 | Berlin |
+----+--------+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_SEMI_JOIN, input2, USING('id'),
                         RIGHT_SEMI_JOIN, input3, USING('id'),
                         RIGHT_SEMI_JOIN, input3, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`3`, () => {
            const table = `
+----+-----+
| id | age |
+----+-----+
|  3 |   1 |
|  4 |  11 |
|  4 |  22 |
+----+-----+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input3, ON('t1.id', 'id'),
                         RIGHT_SEMI_JOIN, input2, ON('t1.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`4`, () => {
            const table = `
+----+-----+
| id | age |
+----+-----+
|  3 |   1 |
|  4 |  11 |
|  4 |  22 |
+----+-----+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         RIGHT_SEMI_JOIN, input2, ON('t1.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`5`, () => {
            const table = `
+----+-----+
| id | age |
+----+-----+
|  3 |   1 |
|  4 |  11 |
|  4 |  22 |
+----+-----+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         RIGHT_SEMI_JOIN, input2, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });


        it(`6`, () => {
            const table = `
+----+-----+
| id | age |
+----+-----+
|  3 |   1 |
|  4 |  11 |
|  4 |  22 |
+----+-----+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         RIGHT_SEMI_JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`7`, () => {
            const table = `
+-------+--------+-------+--------+-------+--------+
| t2.id | t2.age | t3.id | city   | t4.id | t4.age |
+-------+--------+-------+--------+-------+--------+
|     3 |      1 |     3 | Paris  |     3 |      1 |
|     4 |     11 |     4 | Berlin |     4 |     11 |
|     4 |     22 |     4 | Berlin |     4 |     11 |
|     4 |     11 |     4 | Berlin |     4 |     22 |
|     4 |     22 |     4 | Berlin |     4 |     22 |
+-------+--------+-------+--------+-------+--------+
5 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_SEMI_JOIN, input2, ON('id', 'id'),
                         JOIN, input3, ON('id', 'id'),
                         JOIN, input2, ON('t2.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`8`, () => {
            const table = `
+-------+--------+-------+--------+
| t2.id | t2.age | t3.id | t3.age |
+-------+--------+-------+--------+
|     3 |      1 |     3 |      1 |
|     4 |     11 |     4 |     11 |
|     4 |     22 |     4 |     11 |
|     4 |     11 |     4 |     22 |
|     4 |     22 |     4 |     22 |
+-------+--------+-------+--------+
5 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_SEMI_JOIN, input2, ON('id', 'id'),
                         JOIN, input2, ON('id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`9`, () => {
            const table = `
+-------+-----+-------+
| t2.id | age | t3.id |
+-------+-----+-------+
|     3 |   1 |     3 |
|     4 |  11 |     4 |
|     4 |  22 |     4 |
+-------+-----+-------+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_SEMI_JOIN, input2, USING('id'),
                         JOIN, input2, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });


        it(`10`, () => {
            const table = `
+----+--------+--------+
| id | t2.age | t3.age |
+----+--------+--------+
|  3 |      1 |      1 |
|  4 |     11 |     11 |
|  4 |     22 |     11 |
|  4 |     11 |     22 |
|  4 |     22 |     22 |
+----+--------+--------+
5 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_SEMI_JOIN, input2, USING('id'),
                         JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`11`, () => {
            const table = `
+----+-----+
| id | age |
+----+-----+
|  3 |   1 |
|  4 |  11 |
+----+-----+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input11, CROSS_JOIN, input22,
                          RIGHT_SEMI_JOIN, input2, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`12`, () => {
            const table = `
+-------+--------+-------+--------+
| t2.id | t2.age | t3.id | t3.age |
+-------+--------+-------+--------+
|     3 |      1 |     3 |      1 |
|     3 |      1 |     4 |     11 |
|     4 |     11 |     3 |      1 |
|     4 |     11 |     4 |     11 |
|     4 |     22 |     3 |      1 |
|     4 |     22 |     4 |     11 |
+-------+--------+-------+--------+
6 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_SEMI_JOIN, input2, USING('id'),
                         CROSS_JOIN, input22))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });



        it(`13`, () => {
            const table = `
+----+--------+--------+
| id | t3.age | t4.age |
+----+--------+--------+
|  3 |      1 |      1 |
|  4 |     11 |     11 |
|  4 |     22 |     11 |
|  4 |     11 |     22 |
|  4 |     22 |     22 |
+----+--------+--------+
5 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         RIGHT_SEMI_JOIN, input2, ON('t1.id', 'id'),
                         JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`14`, () => {
            const table = `
+-------+--------+-------+--------+
| t3.id | t3.age | t4.id | t4.age |
+-------+--------+-------+--------+
|     3 |      1 |     3 |      1 |
|     4 |     11 |     4 |     11 |
|     4 |     22 |     4 |     11 |
|     4 |     11 |     4 |     22 |
|     4 |     22 |     4 |     22 |
+-------+--------+-------+--------+
5 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         RIGHT_SEMI_JOIN, input2, ON('t1.id', 'id'),
                         JOIN, input2, ON('id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`15`, () => {
            const table = `
+-------+-----+-------+
| t3.id | age | t4.id |
+-------+-----+-------+
|     3 |   1 |     3 |
|     4 |  11 |     4 |
|     4 |  22 |     4 |
+-------+-----+-------+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input11, CROSS_JOIN, input22,
                          RIGHT_SEMI_JOIN, input2, ON('t2.id', 'id'),
                          JOIN, input2, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`16`, () => {
            const table = `
+-------+--------+-------+--------+
| t3.id | t3.age | t4.id | t4.age |
+-------+--------+-------+--------+
|     3 |      1 |     3 |      1 |
|     3 |      1 |     4 |     11 |
|     4 |     11 |     3 |      1 |
|     4 |     11 |     4 |     11 |
|     4 |     22 |     3 |      1 |
|     4 |     22 |     4 |     11 |
+-------+--------+-------+--------+
6 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         RIGHT_SEMI_JOIN, input2, ON('t1.id', 'id'),
                         CROSS_JOIN, input22))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`17`, () => {
            const table = `
+----+-----+
| id | age |
+----+-----+
|  3 |   1 |
|  4 |  11 |
|  4 |  22 |
+----+-----+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_SEMI_JOIN, input2, USING('id'),
                         JOIN, input2, ON('id', 'id'),
                         RIGHT_SEMI_JOIN, input2, ON('t2.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`18`, () => {
            const table = `
+----+-----+
| id | age |
+----+-----+
|  3 |   1 |
|  4 |  11 |
|  4 |  22 |
+----+-----+
3 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_SEMI_JOIN, input2, USING('id'),
                         JOIN, input2, USING('id'),
                         RIGHT_SEMI_JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });
    });









    describe('rightantijoin: ', () => {
        it(`1`, () => {
            const table = `
+----+------+
| id | city |
+----+------+
|  5 | Rome |
+----+------+
1 row selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_ANTI_JOIN, input2, ON('id', 'id'),
                         RIGHT_ANTI_JOIN, input3, ON('id', 'id'),
                         RIGHT_ANTI_JOIN, input3, ON('id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`2`, () => {
const table = `
+----+------+
| id | city |
+----+------+
|  5 | Rome |
+----+------+
1 row selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_ANTI_JOIN, input2, USING('id'),
                         RIGHT_ANTI_JOIN, input3, USING('id'),
                         RIGHT_ANTI_JOIN, input3, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`3`, () => {
            const table = `
+------+-----+
|   id | age |
+------+-----+
| null |  33 |
|    5 |  44 |
|    6 |  55 |
| null |  66 |
+------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         JOIN, input3, ON('t1.id', 'id'),
                         RIGHT_ANTI_JOIN, input2, ON('t1.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`4`, () => {
            const table = `
+------+-----+
|   id | age |
+------+-----+
| null |  33 |
|    5 |  44 |
|    6 |  55 |
| null |  66 |
+------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         RIGHT_ANTI_JOIN, input2, ON('t1.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`5`, () => {
            const table = `
+------+-----+
|   id | age |
+------+-----+
| null |  33 |
|    5 |  44 |
|    6 |  55 |
| null |  66 |
+------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         RIGHT_ANTI_JOIN, input2, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });


        it(`6`, () => {
            const table = `
+------+-----+
|   id | age |
+------+-----+
| null |  33 |
|    5 |  44 |
|    6 |  55 |
| null |  66 |
+------+-----+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         RIGHT_ANTI_JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`7`, () => {
            const table = `
+-------+--------+-------+------+-------+--------+
| t2.id | t2.age | t3.id | city | t4.id | t4.age |
+-------+--------+-------+------+-------+--------+
|     5 |     44 |     5 | Rome |     5 |     44 |
+-------+--------+-------+------+-------+--------+
1 row selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_ANTI_JOIN, input2, ON('id', 'id'),
                         JOIN, input3, ON('id', 'id'),
                         JOIN, input2, ON('t2.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`8`, () => {
            const table = `
+-------+--------+-------+--------+
| t2.id | t2.age | t3.id | t3.age |
+-------+--------+-------+--------+
|     5 |     44 |     5 |     44 |
|     6 |     55 |     6 |     55 |
+-------+--------+-------+--------+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_ANTI_JOIN, input2, ON('id', 'id'),
                         JOIN, input2, ON('id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`9`, () => {
            const table = `
+-------+-----+-------+
| t2.id | age | t3.id |
+-------+-----+-------+
|  null |  33 |  null |
|     5 |  44 |     5 |
|     6 |  55 |     6 |
|  null |  66 |  null |
+-------+-----+-------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_ANTI_JOIN, input2, USING('id'),
                         JOIN, input2, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });


        it(`10`, () => {
            const table = `
+----+--------+--------+
| id | t2.age | t3.age |
+----+--------+--------+
|  5 |     44 |     44 |
|  6 |     55 |     55 |
+----+--------+--------+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_ANTI_JOIN, input2, USING('id'),
                         JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`11`, () => {
            const table = `
+------+-----+
|   id | age |
+------+-----+
|    4 |  22 |
| null |  33 |
|    5 |  44 |
|    6 |  55 |
| null |  66 |
+------+-----+
5 rows selected`;

            const res =
            SELECT('*',
            FROM(input11, CROSS_JOIN, input22,
                          RIGHT_ANTI_JOIN, input2, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`12`, () => {
            const table = `
+-------+--------+-------+--------+
| t2.id | t2.age | t3.id | t3.age |
+-------+--------+-------+--------+
|  null |     33 |     3 |      1 |
|  null |     33 |     4 |     11 |
|     5 |     44 |     3 |      1 |
|     5 |     44 |     4 |     11 |
|     6 |     55 |     3 |      1 |
|     6 |     55 |     4 |     11 |
|  null |     66 |     3 |      1 |
|  null |     66 |     4 |     11 |
+-------+--------+-------+--------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_ANTI_JOIN, input2, USING('id'),
                         CROSS_JOIN, input22))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });



        it(`13`, () => {
            const table = `
+----+--------+--------+
| id | t3.age | t4.age |
+----+--------+--------+
|  5 |     44 |     44 |
|  6 |     55 |     55 |
+----+--------+--------+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, USING('id'),
                         RIGHT_ANTI_JOIN, input2, ON('t1.id', 'id'),
                         JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`14`, () => {
            const table = `
+-------+--------+-------+--------+
| t3.id | t3.age | t4.id | t4.age |
+-------+--------+-------+--------+
|     5 |     44 |     5 |     44 |
|     6 |     55 |     6 |     55 |
+-------+--------+-------+--------+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         RIGHT_ANTI_JOIN, input2, ON('t1.id', 'id'),
                         JOIN, input2, ON('id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`15`, () => {
            const table = `
+-------+-----+-------+
| t3.id | age | t4.id |
+-------+-----+-------+
|  null |  33 |  null |
|     5 |  44 |     5 |
|     6 |  55 |     6 |
|  null |  66 |  null |
+-------+-----+-------+
4 rows selected`;

            const res =
            SELECT('*',
            FROM(input11, CROSS_JOIN, input22,
                          RIGHT_ANTI_JOIN, input2, ON('t2.id', 'id'),
                          JOIN, input2, USING('age')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`16`, () => {
            const table = `
+-------+--------+-------+--------+
| t3.id | t3.age | t4.id | t4.age |
+-------+--------+-------+--------+
|  null |     33 |     3 |      1 |
|  null |     33 |     4 |     11 |
|     5 |     44 |     3 |      1 |
|     5 |     44 |     4 |     11 |
|     6 |     55 |     3 |      1 |
|     6 |     55 |     4 |     11 |
|  null |     66 |     3 |      1 |
|  null |     66 |     4 |     11 |
+-------+--------+-------+--------+
8 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, JOIN, input2, ON('id', 'id'),
                         RIGHT_ANTI_JOIN, input2, ON('t1.id', 'id'),
                         CROSS_JOIN, input22))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`17`, () => {
            const table = `
+------+-----+
|   id | age |
+------+-----+
|    3 |   1 |
|    4 |  11 |
|    4 |  22 |
| null |  33 |
| null |  66 |
+------+-----+
5 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_ANTI_JOIN, input2, USING('id'),
                         JOIN, input2, ON('id', 'id'),
                         RIGHT_ANTI_JOIN, input2, ON('t2.id', 'id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`18`, () => {
            const table = `
+------+-----+
|   id | age |
+------+-----+
|    3 |   1 |
|    4 |  11 |
|    4 |  22 |
| null |  33 |
| null |  66 |
+------+-----+
5 rows selected`;

            const res =
            SELECT('*',
            FROM(input1, RIGHT_ANTI_JOIN, input2, USING('id'),
                         JOIN, input2, USING('id'),
                         RIGHT_ANTI_JOIN, input2, USING('id')))

            expect(trim(drawTable(res))).toEqual(trim(table));
        });
    });


});