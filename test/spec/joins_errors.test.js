import { SELECT, FROM, JOIN, ON, USING, CROSS_JOIN, WHERE, ORDER_BY, DESC } from '../../dist/select.js'
import { input1, input2, input3, input11, input22, input33 } from './data.js'





describe('joins_4 - from_errors:', () => {
    it(`FROM: first parameter is not a table 3`, () => {
        const query = () =>
        SELECT('*',
        FROM(JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: table expected as first parameter'));
    });

    it(`FROM: first parameter is not a table 2`, () => {
        const query = () =>
        SELECT('*',
        FROM(ON('id', 'id'), input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: table expected as first parameter'));
    });

    it(`FROM: first parameter is not a table 1`, () => {
        const query = () =>
        SELECT('*',
        FROM('asd', JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: table expected as first parameter'));
    });

    it(`FROM: first parameter is not a table 4`, () => {
        const query = () =>
        SELECT('*',
        FROM({}, JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: table expected as first parameter'));
    });


    it(`ON follows table`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, ON('id', 'id'), JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after t1 table or JOIN expected'));
    });

    it(`USING follows table`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, USING('id'), JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after t1 table or JOIN expected'));
    });

    it(`ON follows table`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, 'table1', ON('id', 'id'), JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after table1 table or JOIN expected'));
    });

    it(`USING follows table`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, 'table1', USING('id'), JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after table1 table or JOIN expected'));
    });

    it(`another table name follows table name`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, 'table1', 'table2', JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after table1 table or JOIN expected'));
    });

    it(`an object follows table name`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, 'table1', {}, JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after table1 table or JOIN expected'));
    });

    it(`an object follows table`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, {}, JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after t1 table or JOIN expected'));
    });


    it(`join follows join`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, 'table1', JOIN, JOIN, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after JOIN after table1 table expected'));
    });

    it(`on follows join`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, 'table1', JOIN, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after JOIN after table1 table expected'));
    });

    it(`string follows join`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, 'table1', JOIN, 'asd', ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after JOIN after table1 table expected'));
    });

    it(`object follows join`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, 'table1', JOIN, {}, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after JOIN after table1 table expected'));
    });

    it(`object follows join`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, {}, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after JOIN after t1 table expected'));
    });

    it(`table follows input2`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, input1, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after t2 ON/USING expected'));
    });

    it(`join follows input2`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, JOIN, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after t2 ON/USING expected'));
    });

    it(`object follows input2`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, {}, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after t2 ON/USING expected'));
    });

    it(`on follows on`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'), ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after ON/USING after t2 table or JOIN expected'));
    });

    it(`object follows on`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'), {}))

        expect(query).toThrow(new Error('FROM: after ON/USING after t2 table or JOIN expected'));
    });

    it(`string follows on`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'), 'asd'))

        expect(query).toThrow(new Error('FROM: after ON/USING after t2 table or JOIN expected'));
    });

    it(`crossjoin with on 1`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, CROSS_JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after t2 table or JOIN expected'));
    });

    it(`crossjoin with on 2`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after t2 table or JOIN expected'));
    });

    it(`crossjoin with string 1`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, CROSS_JOIN, 'asd', ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after JOIN after t1 table expected'));
    });

    it(`crossjoin with string 2`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, input2, 'asd', ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after asd table or JOIN expected'));
    });

    it(`crossjoin with object 1`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, CROSS_JOIN, {}, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after JOIN after t1 table expected'));
    });

    it(`crossjoin with object 2`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, input2, {}, ON('id', 'id')))

        expect(query).toThrow(new Error('FROM: after t2 table or JOIN expected'));
    });

    it(`unfinished 1`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN))

        expect(query).toThrow(new Error('FROM: after JOIN after t1 table expected'));
    });

    it(`unfinished 2`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2))

        expect(query).toThrow(new Error('FROM: after t2 ON/USING expected'));
    });

    it(`using follows join`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, USING('id')))

        expect(query).toThrow(new Error('FROM: after JOIN after t2 table expected'));
    });

    it(`join follows join`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, JOIN))

        expect(query).toThrow(new Error('FROM: after JOIN after t2 table expected'));
    });

    it(`string follows join`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, 'asd'))

        expect(query).toThrow(new Error('FROM: after JOIN after t2 table expected'));
    });

    it(`object follows join`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, {}))

        expect(query).toThrow(new Error('FROM: after JOIN after t2 table expected'));
    });

    it(`unfinished 3`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input3))

        expect(query).toThrow(new Error('FROM: after t3 ON/USING expected'));
    });
});


describe('joins_5 - column_errors:', () => {
    it(`select non-existing column 1`, () => {
        const query = () =>
        SELECT('city',
        FROM(input1, JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('column city (SELECT 1) does not exist'));
    });

    it(`select non-existing column 2`, () => {
        const query = () =>
        SELECT('name', 'city',
        FROM(input1, JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('column city (SELECT 2) does not exist'));
    });

    it(`select non-existing column 3`, () => {
        const query = () =>
        SELECT('t1.name',
        FROM(input1, JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('column t1.name (SELECT 1) does not exist'));
    });

    it(`select non-existing column 4`, () => {
        const query = () =>
        SELECT('name', 't2.age',
        FROM(input1, JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('column t2.age (SELECT 2) does not exist'));
    });

    it(`select ambiguous column`, () => {
        const query = () =>
        SELECT('id',
        FROM(input1, JOIN, input2, ON('id', 'id')))

        expect(query).toThrow(new Error('column id (SELECT 1) ambiguous'));
    });

    it(`on with non-existing column 1`, () => {
        const query = () =>
        SELECT('id',
        FROM(input1, JOIN, input2, ON('idx', 'id')))

        expect(query).toThrow(new Error('column idx (FROM ON t1) does not exist'));
    });

    it(`on with non-existing column 2`, () => {
        const query = () =>
        SELECT('id',
        FROM(input1, JOIN, input2, ON('id', 'idx')))

        expect(query).toThrow(new Error('column idx (FROM ON t2) does not exist'));
    });

    it(`on with non-existing column 3`, () => {
        const query = () =>
        SELECT('id',
        FROM(input1, JOIN, input2, ON('t1.id', 'id')))

        expect(query).toThrow(new Error('column t1.id (FROM ON t1) does not exist'));
    });

    it(`on with non-existing column 4`, () => {
        const query = () =>
        SELECT('id',
        FROM(input1, JOIN, input2, ON('id', 't2.id')))

        expect(query).toThrow(new Error('column t2.id (FROM ON t2) does not exist'));
    });

    it(`using with non-existing column 1`, () => {
        const query = () =>
        SELECT('id',
        FROM(input1, JOIN, input2, USING('idx')))

        expect(query).toThrow(new Error('column idx (FROM USING t1) does not exist'));
    });

    it(`using with non-existing column 2`, () => {
        const query = () =>
        SELECT('id',
        FROM(input1, JOIN, input2, USING('t1.id')))

        expect(query).toThrow(new Error('column t1.id (FROM USING t1) does not exist'));
    });

    it(`using with non-existing column 3`, () => {
        const query = () =>
        SELECT('id',
        FROM(input1, JOIN, input2, USING('name')))

        expect(query).toThrow(new Error('column name (FROM USING t2) does not exist'));
    });

    it(`using with non-existing column 4`, () => {
        const query = () =>
        SELECT('id',
        FROM(input1, JOIN, input2, USING('age')))

        expect(query).toThrow(new Error('column age (FROM USING t1) does not exist'));
    });

    it(`on without column 1`, () => {
        const query = () =>
        SELECT('city',
        FROM(input1, JOIN, input2, ON()))

        expect(query).toThrow(new Error('string expected (FROM ON t1)'));
    });

    it(`on without column 2`, () => {
        const query = () =>
        SELECT('city',
        FROM(input1, JOIN, input2, ON('')))

        expect(query).toThrow(new Error('string expected (FROM ON t1)'));
    });

    it(`on with only one column`, () => {
        const query = () =>
        SELECT('city',
        FROM(input1, JOIN, input2, ON('id')))

        expect(query).toThrow(new Error('string expected (FROM ON t2)'));
    });

    it(`on with erroneous columns 1`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input3, ON('idx', 'id')))

        expect(query).toThrow(new Error('column idx (FROM ON t1 + t2) does not exist'));
    });

    it(`on with erroneous columns 2`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input3, ON('id', 'id')))

        expect(query).toThrow(new Error('column id (FROM ON t1 + t2) ambiguous'));
    });

    it(`on with erroneous columns 2`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input3, ON('t2.id', 'id'),
                     JOIN, input1, ON('id', 'id')))

        expect(query).toThrow(new Error('column id (FROM ON t1 + t2 + t3) ambiguous'));
    });

    it(`on without column 3`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input3, ON('t2.id', 'id'),
                     JOIN, input1, ON()))

        expect(query).toThrow(new Error('string expected (FROM ON t1 + t2 + t3)'));
    });

    it(`on without column `, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input3, ON('t2.id', 'id'),
                     JOIN, input1, ON('t3.id')))

        expect(query).toThrow(new Error('string expected (FROM ON t4)'));
    });


    it(`on with erroneous columns 3`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input3, ON('t1.id', 't3.id')))

        expect(query).toThrow(new Error('column t3.id (FROM ON t3) does not exist'));
    });

    it(`using with erroneous column`, () => {
        const query = () =>
        SELECT('*',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input3, USING('id')))

        expect(query).toThrow(new Error('column id (FROM USING t1 + t2) ambiguous'));
    });

    it(`err column - 1`, () => {
        const query = () =>
        SELECT('id',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input2, ON('age', 'age'),
                     JOIN, input3, ON('t1.id', 'id'),
                     JOIN, input3, ON('city', 'city')))

        expect(query).toThrow(new Error('column id (SELECT 1) ambiguous'));
    });

    it(`err column - 2`, () => {
        const query = () =>
        SELECT('t1.id', 'age',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input2, ON('age', 'age'),
                     JOIN, input3, ON('t1.id', 'id'),
                     JOIN, input3, ON('city', 'city')))

        expect(query).toThrow(new Error('column age (SELECT 2) ambiguous'));
    });

    it(`err column - 3`, () => {
        const query = () =>
        SELECT('t1.id', 't3.age', 'city',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input2, ON('age', 'age'),
                     JOIN, input3, ON('t1.id', 'id'),
                     JOIN, input3, ON('city', 'city')))

        expect(query).toThrow(new Error('column city (SELECT 3) ambiguous'));
    });

    it(`err column - 4`, () => {
        const query = () =>
        SELECT('t1.id', 't3.age', 't5.city',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input2, ON('age', 'age'),
                     JOIN, input3, ON('t1.id', 'id'),
                     JOIN, input3, ON('city', 'city')),
        WHERE(r => r.id > 2))

        expect(query).toThrow(new Error('column id (WHERE) ambiguous'));
    });

    it(`err column - 5`, () => {
        const query = () =>
        SELECT('t1.id', 't3.age', 't5.city',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input2, ON('age', 'age'),
                     JOIN, input3, ON('t1.id', 'id'),
                     JOIN, input3, ON('city', 'city')),
        WHERE(r => r['t1.id'] > 2 && r.age > 0))

        expect(query).toThrow(new Error('column age (WHERE) ambiguous'));
    });

    it(`err column - 6`, () => {
        const query = () =>
        SELECT('t1.id', 't3.age', 't5.city',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input2, ON('age', 'age'),
                     JOIN, input3, ON('t1.id', 'id'),
                     JOIN, input3, ON('city', 'city')),
        WHERE(r => r['t1.id'] > 2 && r['t3.age'] > 0 && r.city !== 'asd'))

        expect(query).toThrow(new Error('column city (WHERE) ambiguous'));
    });

    it(`err column - 7`, () => {
        const query = () =>
        SELECT('t1.id', 't3.age', 't5.city',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input2, ON('age', 'age'),
                     JOIN, input3, ON('t1.id', 'id'),
                     JOIN, input3, ON('city', 'city')),
        WHERE(r => r['t1.id'] > 2 && r['t3.age'] > 0 && r['t5.city'] !== 'asd'),
        ORDER_BY('id'))

        expect(query).toThrow(new Error('column id (ORDER_BY) ambiguous'));
    });

    it(`err column - 8`, () => {
        const query = () =>
        SELECT('t1.id', 't3.age', 't5.city',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input2, ON('age', 'age'),
                     JOIN, input3, ON('t1.id', 'id'),
                     JOIN, input3, ON('city', 'city')),
        WHERE(r => r['t1.id'] > 2 && r['t3.age'] > 0 && r['t5.city'] !== 'asd'),
        ORDER_BY('t3.id', 'age'))

        expect(query).toThrow(new Error('column age (ORDER_BY) ambiguous'));
    });

    it(`err column - 9`, () => {
        const query = () =>
        SELECT('t1.id', 't3.age', 't5.city',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input2, ON('age', 'age'),
                     JOIN, input3, ON('t1.id', 'id'),
                     JOIN, input3, ON('city', 'city')),
        WHERE(r => r['t1.id'] > 2 && r['t3.age'] > 0 && r['t5.city'] !== 'asd'),
        ORDER_BY('t3.id', 't3.age', 'city'))

        expect(query).toThrow(new Error('column city (ORDER_BY) ambiguous'));
    });

    it(`missing column - 1`, () => {
        const query = () =>
        SELECT('t1.id', 'idx',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input2, ON('age', 'age'),
                     JOIN, input3, ON('t1.id', 'id'),
                     JOIN, input3, ON('city', 'city')))

        expect(query).toThrow(new Error('column idx (SELECT 2) does not exist'));
    });

    it(`missing column - 2`, () => {
        const query = () =>
        SELECT('t1.id',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input2, ON('age', 'age'),
                     JOIN, input3, ON('t1.id', 'id'),
                     JOIN, input3, ON('city', 'city')),
        WHERE(r => r.idx > 1))

        expect(query).toThrow(new Error('column idx (WHERE) does not exist'));
    });

    it(`missing column - 3`, () => {
        const query = () =>
        SELECT('t1.id',
        FROM(input1, JOIN, input2, ON('id', 'id'),
                     JOIN, input2, ON('age', 'age'),
                     JOIN, input3, ON('t1.id', 'id'),
                     JOIN, input3, ON('city', 'city')),
        WHERE(r => r['t1.id'] > 1),
        ORDER_BY('idx'))

        expect(query).toThrow(new Error('column idx (ORDER_BY) does not exist'));
    });



    it(``, () => {
        const query = () =>
        SELECT('id', 'name', 't5.id', 'idx',
        FROM(input1, JOIN, input2, USING('id'),
                     JOIN, input2, USING('age'),
                     JOIN, input3, USING('id'),
                     JOIN, input3, USING('city')))

        expect(query).toThrow(new Error('column idx (SELECT 4) does not exist'));
    });

    it(``, () => {
        const query = () =>
        SELECT('id', 'name', 't5.id',
        FROM(input1, JOIN, input2, USING('id'),
                     JOIN, input2, USING('age'),
                     JOIN, input3, USING('id'),
                     JOIN, input3, USING('city')),
        WHERE(r => r.idx > 0))

        expect(query).toThrow(new Error('column idx (WHERE) does not exist'));
    });

    it(``, () => {
        const query = () =>
        SELECT('id', 'name', 't5.id',
        FROM(input1, JOIN, input2, USING('id'),
                     JOIN, input2, USING('age'),
                     JOIN, input3, USING('id'),
                     JOIN, input3, USING('city')),
        WHERE(r => r.id > 0),
        ORDER_BY('idx'))

        expect(query).toThrow(new Error('column idx (ORDER_BY) does not exist'));
    });

    it(``, () => {
        const query = () =>
        SELECT('name', 'id', 't1.id', 'city',
        FROM(input1, JOIN, input2, USING('id'),
                     JOIN, input3, USING('id'),
                     JOIN, input3, ON('t1.id', 'id')),
        WHERE(r => r.id > 0 && r['t4.id'] > 0),
        ORDER_BY('age', 't3.city', DESC, 't4.city', DESC))
         
        expect(query).toThrow(new Error('column city (SELECT 4) ambiguous'));
    });

    it(``, () => {
        const query = () =>
        SELECT('name', 'id', 't1.id',
        FROM(input1, JOIN, input2, USING('id'),
                     JOIN, input3, USING('id'),
                     JOIN, input3, ON('t1.id', 'id')),
        WHERE(r => r.id > 0 && r['t4.id'] > 0 && r.city > 'A'),
        ORDER_BY('age', 't3.city', DESC, 't4.city', DESC))
         
        expect(query).toThrow(new Error('column city (WHERE) ambiguous'));
    });

    it(``, () => {
        const query = () =>
        SELECT('name', 'id', 't1.id',
        FROM(input1, JOIN, input2, USING('id'),
                     JOIN, input3, USING('id'),
                     JOIN, input3, ON('t1.id', 'id')),
        WHERE(r => r.id > 0 && r['t4.id'] > 0),
        ORDER_BY('age', 'city', DESC, 't4.city', DESC))
         
        expect(query).toThrow(new Error('column city (ORDER_BY) ambiguous'));
    });

    it(``, () => {
        const query = () =>
        SELECT('name', 'id', 't1.id', 'cityx',
        FROM(input1, JOIN, input2, USING('id'),
                     JOIN, input3, USING('id'),
                     JOIN, input3, ON('t1.id', 'id')),
        WHERE(r => r.id > 0 && r['t4.id'] > 0),
        ORDER_BY('age', 't3.city', DESC, 't4.city', DESC))
         
        expect(query).toThrow(new Error('column cityx (SELECT 4) does not exist'));
    });

    it(``, () => {
        const query = () =>
        SELECT('name', 'id', 't1.id',
        FROM(input1, JOIN, input2, USING('id'),
                     JOIN, input3, USING('id'),
                     JOIN, input3, ON('t1.id', 'id')),
        WHERE(r => r.id > 0 && r['t4.id'] > 0 && r.cityx > 'A'),
        ORDER_BY('age', 't3.city', DESC, 't4.city', DESC))
         
        expect(query).toThrow(new Error('column cityx (WHERE) does not exist'));
    });

    it(``, () => {
        const query = () =>
        SELECT('name', 'id', 't1.id',
        FROM(input1, JOIN, input2, USING('id'),
                     JOIN, input3, USING('id'),
                     JOIN, input3, ON('t1.id', 'id')),
        WHERE(r => r.id > 0 && r['t4.id'] > 0),
        ORDER_BY('age', 'cityx', DESC, 't4.city', DESC))
         
        expect(query).toThrow(new Error('column cityx (ORDER_BY) does not exist'));
    });

    it(``, () => {            
        const query = () =>
        SELECT('id',
        FROM(input11, CROSS_JOIN, input22,
                      CROSS_JOIN, input33,
                      JOIN, input33, ON('t2.id', 'id')),
        WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
        ORDER_BY('name', 'age'))
          
        expect(query).toThrow(new Error('column id (SELECT 1) ambiguous'));
    });

    it(``, () => {            
        const query = () =>
        SELECT('t1.id',
        FROM(input11, CROSS_JOIN, input22,
                      CROSS_JOIN, input33,
                      JOIN, input33, ON('t2.id', 'id')),
        WHERE(r => r['id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
        ORDER_BY('name', 'age'))
          
        expect(query).toThrow(new Error('column id (WHERE) ambiguous'));
    });

    it(``, () => {            
        const query = () =>
        SELECT('t1.id',
        FROM(input11, CROSS_JOIN, input22,
                      CROSS_JOIN, input33,
                      JOIN, input33, ON('t2.id', 'id')),
        WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
        ORDER_BY('name', 'age', 'city'))
          
        expect(query).toThrow(new Error('column city (ORDER_BY) ambiguous'));
    });

    it(``, () => {            
        const query = () =>
        SELECT('idx',
        FROM(input11, CROSS_JOIN, input22,
                      CROSS_JOIN, input33,
                      JOIN, input33, ON('t2.id', 'id')),
        WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
        ORDER_BY('name', 'age'))
          
        expect(query).toThrow(new Error('column idx (SELECT 1) does not exist'));
    });

    it(``, () => {            
        const query = () =>
        SELECT('t1.id',
        FROM(input11, CROSS_JOIN, input22,
                      CROSS_JOIN, input33,
                      JOIN, input33, ON('t2.id', 'id')),
        WHERE(r => r['idx'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
        ORDER_BY('name', 'age'))
          
        expect(query).toThrow(new Error('column idx (WHERE) does not exist'));
    });

    it(``, () => {            
        const query = () =>
        SELECT('t1.id',
        FROM(input11, CROSS_JOIN, input22,
                      CROSS_JOIN, input33,
                      JOIN, input33, ON('t2.id', 'id')),
        WHERE(r => r['t1.id'] > 0 && r['t2.id'] > 0 && r['t3.id'] > 0),
        ORDER_BY('name', 'age', 'idx'))
          
        expect(query).toThrow(new Error('column idx (ORDER_BY) does not exist'));
    });


});
