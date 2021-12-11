import { SELECT, WHERE, FROM, ORDER_BY, DESC, ASC, NOCASE_ASC,
         NOCASE_DESC, DESC_LOC } from '../../dist/select.js'
import { drawTable } from './utils.js'
const trim = str => str.trim();


describe('no funcions:', () => {
    describe('Basic selects:', () => {
        it(`smallest correct select`, () => {
            const input = [];

            const res = 
            SELECT('*',
            FROM(input));

            expect(res).toEqual(input);
            expect(res).not.toBe(input);
        });

        it(`second smallest correct select`, () => {
            const input = [{}];

            const res =
            SELECT('*',
            FROM(input));

            expect(res).toEqual(input);
            expect(res).not.toBe(input);
            expect(res[0]).toEqual(input[0]);
            expect(res[0]).not.toBe(input[0]);
        });

        it(`select with unexisting column 1`, () => {
            const res =
            SELECT('asd',
            FROM([]))

            expect(res).toEqual([]);
        });

        it(`select with unexisting column 2`, () => {
            const query = () =>
            SELECT('asd',
            FROM([{}]))

            expect(query).toThrow(new Error('column asd (SELECT 1) does not exist'));
        });

        it(`undefined => null`, () => {
            const input = [
                { a: 1, b: 2, c: undefined },
                { a: 4, b: 5, c: 6 },
                { a: 7, b: 8, c: 9 }
            ]

            const table = `
+---+---+------+
| a | b |    c |
+---+---+------+
| 1 | 2 | null |
| 4 | 5 |    6 |
| 7 | 8 |    9 |
+---+---+------+
3 rows selected`;
            
            const res =
            SELECT('*',
            FROM(input))
    
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`less column in second row -> undefined => null`, () => {
            const input = [ 
                { a: 1, b: 2, c: 3 }, 
                { a: 4, b: 5 }, 
                { a: 7, b: 8, c: 9 } 
            ]

            const table = `
+---+---+------+
| a | b |    c |
+---+---+------+
| 1 | 2 |    3 |
| 4 | 5 | null |
| 7 | 8 |    9 |
+---+---+------+
3 rows selected`;
            
            const res =
            SELECT('*',
            FROM(input))
    
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`more column in second row -> ignored`, () => {
            const input = [ 
                { a: 1, b: 2, c: 3 }, 
                { a: 4, b: 5, c: 6, d: 111 }, 
                { a: 7, b: 8, c: 9 } 
            ]

            const table = `
+---+---+---+
| a | b | c |
+---+---+---+
| 1 | 2 | 3 |
| 4 | 5 | 6 |
| 7 | 8 | 9 |
+---+---+---+
3 rows selected
`;
                        
            const res =
            SELECT('*',
            FROM(input))
    
            expect(trim(drawTable(res))).toEqual(trim(table));
        });


    });

    describe('WHERE:', () => {
        const input = [ 
            { a: 1, b: 2, c: 3 }, 
            { a: 4, b: 5, c: 6 }, 
            { a: 7, b: 8, c: 9 } 
        ]

        it(`with indexed prop`, () => {
            const table = `
+---+---+---+
| a | b | c |
+---+---+---+
| 4 | 5 | 6 |
| 7 | 8 | 9 |
+---+---+---+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input),
            WHERE(r => r['b'] > 2))
    
            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`with method prop`, () => {
            const table = `
+---+---+---+
| a | b | c |
+---+---+---+
| 4 | 5 | 6 |
| 7 | 8 | 9 |
+---+---+---+
2 rows selected`;

            const res =
            SELECT('*',
            FROM(input),
            WHERE(r => r.b > 2))
    
            expect(trim(drawTable(res))).toEqual(trim(table));
        });



    });



    describe('WHERE + ORDER_BY:', () => {
        const input = [ 
            { a: 1, b: 1, c: 1 },
            { a: 2, b: 1, c: 1 },
            { a: 2, b: 1, c: 2 },
            { a: 1, b: 2, c: 2 },
            { a: 2, b: 2, c: 1 },
            { a: 1, b: 2, c: 1 },
            { a: 2, b: 2, c: 2 },
            { a: 1, b: 1, c: 2 }
        ]

        it('', () => {
            const table = `
+---+---+---+
| a | b | c |
+---+---+---+
| 2 | 1 | 2 |
| 2 | 1 | 1 |
| 2 | 2 | 2 |
| 2 | 2 | 1 |
+---+---+---+
4 rows selected`;
                    
            const res =
            SELECT('a', 'b', 'c',
            FROM(input),
            WHERE(r => r.a > 1),
            ORDER_BY('a', 'b', ASC, 'c', DESC));
    
            expect(trim(drawTable(res))).toEqual(trim(table));
            
        });


    });



    describe('Exceptions - basic selects:', () => {
        it(`no parameters`, () => {
            const query = () =>
            SELECT()

            expect(query).toThrow(new Error('FROM must be supplied'));
        });

        it(`no clause`, () => {
            const query = () =>
            SELECT('*')

            expect(query).toThrow(new Error('FROM must be supplied'));
        });

        it(`clause other than FROM`, () => {
            const query = () =>
            SELECT('*',
            WHERE(row => row))

            expect(query).toThrow(new Error('FROM must be supplied'));
        });

        it(`FROM without selected column`, () => {
            const query = () =>
            SELECT(FROM())

            expect(query).toThrow(new Error('no column selected'));
        });

        it(`FROM without parameter`, () => {
            const query = () =>
            SELECT('*',
            FROM())

            expect(query).toThrow(new Error('FROM: table expected as first parameter'));
        });

        it(`FROM with parameter other than table (string)`, () => {
            const query = () =>
            SELECT('*',
            FROM('asd'))

            expect(query).toThrow(new Error('FROM: table expected as first parameter'));
        });


        it(`select: erroneous selected column 1`, () => {
            const query = () =>
            SELECT('',
            FROM([{ 'asd': 1 }]))

            expect(query).toThrow(new Error('empty string (SELECT 1)'));
        });
    });

    describe('Exceptions - WHERE', () => {
        it(`where: erroneous column 1`, () => {
            const query = () =>
            SELECT('*',
            FROM([{ 'asd': 1 }]),
            WHERE())

            expect(query).toThrow(new Error('predicate expected (WHERE)'));
        });

        it(`where: erroneous column 2`, () => {
            const query = () =>
            SELECT('*',
            FROM([{ 'asd': 1 }]),
            WHERE(() => r['asd'] > 0))

            expect(query).toThrow(new Error('parameter expected in predicate of WHERE'));
        });

        it(`where: erroneous column 3`, () => {
            const query = () =>
            SELECT('*',
            FROM([{ 'asd': 1 }]),
            WHERE(r => r[''] > 0))

            expect(query).toThrow(new Error('empty string (WHERE)'));
        });


    });

    describe('Exceptions - ORDER_BY', () => {
        it(`orderby: erroneous column 1`, () => {
            const query = () =>
            SELECT('*',
            FROM([{ 'asd': 1 }]),
            ORDER_BY())

            expect(query).toThrow(new Error('string expected (ORDER_BY)'));
        });

        it(`orderby: erroneous column 2`, () => {
            const query = () =>
            SELECT('*',
            FROM([{ 'asd': 1 }]),
            ORDER_BY(''))

            expect(query).toThrow(new Error('string expected (ORDER_BY)'));
        });

    });


    describe('Exceptions - WHERE + ORDER_BY', () => {
        const input = [ 
            { a: 1, b: 2, c: 3 }, 
            { a: 4, b: 5, c: 6 }, 
            { a: 7, b: 8, c: 9 } ]

        it(`SELECT with unexisting column`, () => {
            const query = () =>
            SELECT('ax',
            FROM(input))

            expect(query).toThrow(new Error('column ax (SELECT 1) does not exist'));
        });

        it(`WHERE with unexisting column`, () => {
            const query = () =>
            SELECT('a',
            FROM(input),
            WHERE(r => r.ax > 2))

            expect(query).toThrow(new Error('column ax (WHERE) does not exist'));
        });

        it(`ORDER_BY with unexisting column`, () => {
            const query = () =>
            SELECT('a',
            FROM(input),
            WHERE(r => r.a > 2),
            ORDER_BY('ax'))

            expect(query).toThrow(new Error('column ax (ORDER_BY) does not exist'));
        });

        it(`SELECT Clause: wrong order 1`, () => {
            const query = () =>
            SELECT('a',
            WHERE(r => r.a > 2),
            FROM(input),
            ORDER_BY('a'))

            expect(query).toThrow(new Error('SELECT clauses: wrong order'));
        });

        it(`SELECT Clause: wrong order 2`, () => {
            const query = () =>
            SELECT('a',
            FROM(input),
            ORDER_BY('a'),
            WHERE(r => r.a > 2))

            expect(query).toThrow(new Error('SELECT clauses: wrong order'));
        });

        it(`SELECT Clause: duplicates 1`, () => {
            const query = () =>
            SELECT('a',
            FROM(input),
            WHERE(r => r.a > 2),
            WHERE(r => r.a > 2),
            ORDER_BY('a'))

            expect(query).toThrow(new Error('SELECT clauses: duplicates'));
        });

        it(`SELECT Clause: duplicates 2`, () => {
            const query = () =>
            SELECT('a',
            FROM(input),
            WHERE(r => r.a > 2),
            ORDER_BY('a'),
            ORDER_BY('a'))

            expect(query).toThrow(new Error('SELECT clauses: duplicates'));
        });

        it(`SELECT: duplicate columns`, () => {
            const query = () =>
            SELECT('a', 'b', 'a',
            FROM(input),
            WHERE(r => r.a > 2),
            ORDER_BY('a'))

            expect(query).toThrow(new Error('column a (SELECT 3) already exists'));
        });


    });


});


describe('no funcions orderby:', () => {
    describe('ORDER_BY', () => {
        const input = [ 
            { a: 1, b: 1, c: 1 },
            { a: 2, b: 1, c: 1 },
            { a: 2, b: 1, c: 2 },
            { a: 1, b: 2, c: 2 },
            { a: 2, b: 2, c: 1 },
            { a: 1, b: 2, c: 1 },
            { a: 2, b: 2, c: 2 },
            { a: 1, b: 1, c: 2 }
        ]

        const inputStr = [
            { a: 'a' },
            { a: 'C' },
            { a: 'b' },
            { a: 'c' },
            { a: 'D' },
            { a: 'd' },
            { a: 'B' },
            { a: 'A' },
        ]
        it(`ORDER_BY ASC ASC ASC`, () => {
            const table = `
+---+---+---+
| a | b | c |
+---+---+---+
| 1 | 1 | 1 |
| 1 | 1 | 2 |
| 1 | 2 | 1 |
| 1 | 2 | 2 |
| 2 | 1 | 1 |
| 2 | 1 | 2 |
| 2 | 2 | 1 |
| 2 | 2 | 2 |
+---+---+---+
8 rows selected`;

            const res =
            SELECT('a', 'b', 'c',
            FROM(input),
            ORDER_BY('a', 'b', 'c'));

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`ORDER_BY ASC DESC ASC`, () => {
            const table = `
+---+---+---+
| a | b | c |
+---+---+---+
| 1 | 2 | 1 |
| 1 | 2 | 2 |
| 1 | 1 | 1 |
| 1 | 1 | 2 |
| 2 | 2 | 1 |
| 2 | 2 | 2 |
| 2 | 1 | 1 |
| 2 | 1 | 2 |
+---+---+---+
8 rows selected`;

            const res =
            SELECT('a', 'b', 'c',
            FROM(input),
            ORDER_BY('a', 'b', DESC, 'c'));

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`ORDER_BY ASC NOCASE_DESC ASC -> no error`, () => {
            const table = `
+---+---+---+
| a | b | c |
+---+---+---+
| 1 | 2 | 1 |
| 1 | 2 | 2 |
| 1 | 1 | 1 |
| 1 | 1 | 2 |
| 2 | 2 | 1 |
| 2 | 2 | 2 |
| 2 | 1 | 1 |
| 2 | 1 | 2 |
+---+---+---+
8 rows selected`;

            const res =
            SELECT('a', 'b', 'c',
            FROM(input),
            ORDER_BY('a', 'b', NOCASE_DESC, 'c'));

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`ORDER_BY ASC DESC_LOC ASC -> no error`, () => {
            const table = `
+---+---+---+
| a | b | c |
+---+---+---+
| 1 | 2 | 1 |
| 1 | 2 | 2 |
| 1 | 1 | 1 |
| 1 | 1 | 2 |
| 2 | 2 | 1 |
| 2 | 2 | 2 |
| 2 | 1 | 1 |
| 2 | 1 | 2 |
+---+---+---+
8 rows selected`;

            const res =
            SELECT('a', 'b', 'c',
            FROM(input),
            ORDER_BY('a', 'b', DESC_LOC, 'c'));

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`String - ASC`, () => {
            const table = `
+---+
| a |
+---+
| A |
| B |
| C |
| D |
| a |
| b |
| c |
| d |
+---+
8 rows selected
`;

            const res =
            SELECT('a',
            FROM(inputStr),
            ORDER_BY('a', ASC));

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`String - DESC`, () => {
            const table = `
+---+
| a |
+---+
| d |
| c |
| b |
| a |
| D |
| C |
| B |
| A |
+---+
8 rows selected
`;

            const res =
            SELECT('a',
            FROM(inputStr),
            ORDER_BY('a', DESC));

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`String - NOCASE_ASC`, () => {
            const table = `
+---+
| a |
+---+
| a |
| A |
| b |
| B |
| C |
| c |
| D |
| d |
+---+
8 rows selected`;

            const res =
            SELECT('a',
            FROM(inputStr),
            ORDER_BY('a', NOCASE_ASC));

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`String - NOCASE_DESC`, () => {
            const table = `
+---+
| a |
+---+
| D |
| d |
| C |
| c |
| b |
| B |
| a |
| A |
+---+
8 rows selected
`;

            const res =
            SELECT('a',
            FROM(inputStr),
            ORDER_BY('a', NOCASE_DESC));

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

// local dependent tests:
//         it(`String - ASC_LOC`, () => {
//             const table = `
// +---+
// | a |
// +---+
// | a |
// | A |
// | b |
// | B |
// | c |
// | C |
// | d |
// | D |
// +---+
// 8 rows selected
// `;

//             const res =
//             SELECT('a',
//             FROM(inputStr),
//             ORDER_BY('a', ASC_LOC));

//             expect(trim(drawTable(res))).toEqual(trim(table));
//         });

//         it(`String - DESC_LOC`, () => {
//             const table = `
// +---+
// | a |
// +---+
// | D |
// | d |
// | C |
// | c |
// | B |
// | b |
// | A |
// | a |
// +---+
// 8 rows selected
// `;

//             const res =
//             SELECT('a',
//             FROM(inputStr),
//             ORDER_BY('a', DESC_LOC));

//             expect(trim(drawTable(res))).toEqual(trim(table));
//         });
    });

    describe('ORDER_BY with NULL', () => {
        const input = [ 
            { a: 1 },
            { a: 2 },
            { a: 2 },
            { a: null },
            { a: 2 },
            { a: 1 },
            { a: null },
            { a: 1 }
        ]

        const inputStr = [
            { a: 'a' },
            { a: 'C' },
            { a: null },
            { a: 'c' },
            { a: null },
            { a: 'd' },
            { a: 'B' },
            { a: 'A' }
        ]
        it(`ORDER_BY ASC ASC ASC`, () => {
            const table = `
+------+
|    a |
+------+
| null |
| null |
|    1 |
|    1 |
|    1 |
|    2 |
|    2 |
|    2 |
+------+
8 rows selected
`;

            const res =
            SELECT('a',
            FROM(input),
            ORDER_BY('a'));

            expect(trim(drawTable(res))).toEqual(trim(table));
        });

        it(`String - DESC`, () => {
            const table = `
+------+
| a    |
+------+
| d    |
| c    |
| a    |
| C    |
| B    |
| A    |
| null |
| null |
+------+
8 rows selected
`;

            const res =
            SELECT('a',
            FROM(inputStr),
            ORDER_BY('a', DESC));

            expect(trim(drawTable(res))).toEqual(trim(table));
        });


    
    });

    describe('Exceptions - ORDER_BY', () => {
        const input = [ 
            { a: 1, b: 1, c: 1 },
            { a: 2, b: 1, c: 1 },
            { a: 2, b: 1, c: 2 },
            { a: 1, b: 2, c: 2 },
            { a: 2, b: 2, c: 1 },
            { a: 1, b: 2, c: 1 },
            { a: 2, b: 2, c: 2 },
            { a: 1, b: 1, c: 2 }
        ]
        it(`orderby: erroneous column 1`, () => {
            const query = () =>
            SELECT('a', 'b', 'c',
            FROM(input),
            ORDER_BY('a', 'b', ASC, DESC, 'c'))

            expect(query).toThrow(new Error('invalid parameters (ORDER_BY)'));
        });

        it(`orderby: erroneous column 2`, () => {
            const query = () =>
            SELECT('a', 'b', 'c',
            FROM(input),
            ORDER_BY(ASC, 'a', 'b', ASC, 'c'))

            expect(query).toThrow(new Error('invalid parameters (ORDER_BY)'));
        });

        it(`orderby: erroneous column 3`, () => {
            const query = () =>
            SELECT('a', 'b', 'c',
            FROM(input),
            ORDER_BY('a', 'b', ASC, 'c', ASC, ASC))

            expect(query).toThrow(new Error('invalid parameters (ORDER_BY)'));
        });

    });

});