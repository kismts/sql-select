export const JOIN: object;
export const INNER_JOIN: object;
export const CROSS_JOIN: object;
export const LEFT_JOIN: object;
export const RIGHT_JOIN: object;
export const FULL_JOIN: object;

export const ON: (str1: string, str2: string) => object;
export const USING: (str: string) => object;

export const ORDER_BY: (...columns: (string | object)[]) => object;


export const COUNT: (p: string | ((...ps:any[]) => any),
                    AS_WIN?: (str: string) => object | ((...clauses: object[]) => object),
                    AS?: (str: string) => object) => object;

export const SUM: (p: string | ((...ps:any[]) => any),
                    AS_WIN?: (str: string) => object | ((...clauses: object[]) => object),
                    AS?: (str: string) => object) => object;

export const AVG: (p: string | ((...ps:any[]) => any),
                    AS_WIN?: (str: string) => object | ((...clauses: object[]) => object),
                    AS?: (str: string) => object) => object;

export const MIN: (p: string | ((...ps:any[]) => any),
                    AS_WIN?: (str: string) => object | ((...clauses: object[]) => object),
                    AS?: (str: string) => object) => object;

export const MAX: (p: string | ((...ps:any[]) => any),
                    AS_WIN?: (str: string) => object | ((...clauses: object[]) => object),
                    AS?: (str: string) => object) => object;

export const COUNT_DISTINCT: (p: string | ((...ps:any[]) => any), AS?: (str: string) => object) => object;
export const SUM_DISTINCT: (p: string | ((...ps:any[]) => any), AS?: (str: string) => object) => object;
export const AVG_DISTINCT: (p: string | ((...ps:any[]) => any), AS?: (str: string) => object) => object;

export const AS: (str: string) => object;

export const ID: (p: any, AS?: (str: string) => object) => any;
export const LEN: (p: string, AS?: (str: string) => object) => any;
export const ROUND: (p: number, AS?: (str: string) => object) => any;

export const ADD: (...ps: (string | number | object)[]) => string | number;
export const SUB: (...ps: (string | number | object)[]) => string | number;
export const MUL: (...ps: (string | number | object)[]) => string | number;
export const DIV: (...ps: (string | number | object)[]) => string | number;

export const FROM: (...tables: object[][]) => object[];
export const WHERE: (pred: (obj: object) => boolean) => object;
export const GROUP_BY: (...columns: string[]) => object;
export const HAVING: (pred: (obj: object) => boolean) => object;

export const ASC: object;
export const DESC: object;
export const NOCASE_ASC: object;
export const NOCASE_DESC: object;
export const ASC_LOC: object;
export const DESC_LOC: object;

export const SELECT: (...columns: any[]) => object[];
export const SELECT_TOP: (n: number) => (...columns: any[]) => object[];

export const SELECT_DISTINCT: (...columns: any[]) => object[];
export const SELECT_DISTINCT_TOP: (n: number) => (...columns: any[]) => object[];

export const createFn: (fn: any) => (...ps: any[]) => any;
