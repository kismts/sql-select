import { SELECT, FROM, ADD, AS } from '../../dist/select.js'


describe('table column names:', () => {
    it(`select: erroneous table column 1`, () => {
        const query = () =>
        SELECT('*',
        FROM([{ '': 1 }]))

        expect(query).toThrow(new Error('empty string (t1 column)'));
    });

    it(`select: erroneous table column 2`, () => {
        const query = () =>
        SELECT('*',
        FROM([{ ' asd': 1 }]))

        expect(query).toThrow(new Error('asd: leading/trailing space (t1 column)'));
    });

    it(`select: erroneous table column 3`, () => {
        const query = () =>
        SELECT('*',
        FROM([{ '*': 1 }]))

        expect(query).toThrow(new Error('asterisk (t1 column)'));
    });

    it(`select: erroneous table column 4`, () => {
        const query = () =>
        SELECT('*',
        FROM([{ 'asd.asd': 1 }]))

        expect(query).toThrow(new Error('asd.asd: point character (t1 column)'));
    });


    it(`select: erroneous table column 5`, () => {
        const query = () =>
        SELECT('*',
        FROM([{ '123': 1 }]))

        expect(query).toThrow(new Error('123: all digits (t1 column)'));
    });

    it(`select: erroneous table column 6`, () => {
        const query = () =>
        SELECT('*',
        FROM([{ ':asd:': 1 }]))

        expect(query).toThrow(new Error(':asd:: invalid name (t1 column)'));
    });







});

describe('table names:', () => {
    it(`empty string table name`, () => {
        const query = () =>
        SELECT('*',
        FROM([], ''))

        expect(query).toThrow(new Error('empty string (FROM table-name)'));
    });

});

describe('column alias:', () => {
    it(`erroneous column alias 1`, () => {
        const query = () =>
        SELECT(ADD('asd', 'bsd'),
        FROM([{ asd: 1, bsd: 2 }]))

        expect(query).toThrow(new Error('AS expected (SELECT 1)'));
    });

    it(`erroneous column alias 2`, () => {
        const query = () =>
        SELECT(ADD('asd', 'bsd', AS()),
        FROM([{ asd: 1, bsd: 2 }]))

        expect(query).toThrow(new Error('string expected (SELECT 1 AS)'));
    });

    it(`erroneous column alias 3`, () => {
        const query = () =>
        SELECT(ADD('asd', 'bsd', AS('')),
        FROM([{ asd: 1, bsd: 2 }]))

        expect(query).toThrow(new Error('string expected (SELECT 1 AS)'));
    });

    it(`erroneous column alias 4`, () => {
        const query = () =>
        SELECT(ADD('asd', 'bsd', AS('*')),
        FROM([{ asd: 1, bsd: 2 }]))

        expect(query).toThrow(new Error('asterisk (SELECT 1 AS)'));
    });

    it(`erroneous column alias 5`, () => {
        const query = () =>
        SELECT(ADD('asd', 'bsd', AS('asd.asd')),
        FROM([{ asd: 1, bsd: 2 }]))

        expect(query).toThrow(new Error('asd.asd: point character (SELECT 1 AS)'));
    });

    it(`erroneous column alias 5`, () => {
        const query = () =>
        SELECT(ADD('asd', 'bsd', AS('123')),
        FROM([{ asd: 1, bsd: 2 }]))

        expect(query).toThrow(new Error('123: all digits (SELECT 1 AS)'));
    });

    it(`erroneous column alias 6`, () => {
        const query = () =>
        SELECT(ADD('asd', 'bsd', AS(':asd:')),
        FROM([{ asd: 1, bsd: 2 }]))

        expect(query).toThrow(new Error(':asd:: invalid name (SELECT 1 AS)'));
    });

    it(`erroneous column alias 7`, () => {
        const query = () =>
        SELECT(ADD('asd', 'bsd', AS('toString')),
        FROM([{ asd: 1, bsd: 2 }]))

        expect(query).toThrow(new Error('toString: invalid name (SELECT 1 AS)'));
    });


});




