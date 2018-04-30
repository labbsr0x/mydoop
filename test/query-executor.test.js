const assert = require('assert');
const queryExecutor = require('../src/query-executor')

describe('queryExecutor', () => {

    describe('#execute()', () => {
        it('should execute a NONE query and return a resultset as array', async () => {
            const query = {
                sql: `select table_mock_alias.column_1 as "1",table_mock_alias.column_2 as "2",-1 as "3",count(*) as "4",sum(table_mock_alias.column_4) as "5",max(table_mock_alias.column_4) as "6",sum(table_mock_alias.column_5) as "7",sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end) as "8",sum(case table_mock_alias.column_6 when 1 then 1 else 0 end) as "9",-1 as "10" from  table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') group by table_mock_alias.column_1, table_mock_alias.column_2`,
                aggregationType: 'NONE',
                distinct: true,
                role: 'MASTER',
                term: "FULL",
                targetColumn: "NONE"                
            }
            const res = await queryExecutor.executeInDatabase(query)            
            assert.equal(res.length, 3)
        })
        it('should execute a COUNT query and return a resultset as array', async () => {
            const query = {
                sql: `select distinct table_mock_alias.column_3 as "3" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1='COL--1' and table_mock_alias.column_2='COL--2'`,
                aggregationType: 'COUNT',
                distinct: true,
                role: 'AGGREGATION',
                term: 'count(distinct table_mock_alias.column_3)',
                targetColumn: 'table_mock_alias.column_3'
            }
            const res = await queryExecutor.executeInDatabase(query)
            assert.equal(res.length, 3)
        })
        it('should execute a COUNT query and return a resultset as array', async () => {
            const query = {
                sql: `select distinct table_mock_alias.column_8 as "10" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1='COL--1' and table_mock_alias.column_2='COL--2'`,
                aggregationType: 'COUNT',
                distinct: true,
                role: 'AGGREGATION',
                term: 'count(distinct table_mock_alias.column_8)',
                targetColumn: 'table_mock_alias.column_8'
            }
            const res = await queryExecutor.executeInDatabase(query)
            assert.equal(res.length, 2)
        })
    })
    describe('#mergeSQLWithParams()', () => {
        it('should keep the SQL unmodified as it doesnt has special params', () => {
            const targetQuery = `select distinct table_mock_alias.column_3 as "3" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44')`
            const resultSetMock = {
                '1': 'COL--1',
                '2': 'COL--2',
                '3': -1,
                '4': 4,
                '5': 268,
                '6': 222,
                '7': 401,
                '8': 0,
                '9': 0,
                '10': -1
            }
            const finalQuery = queryExecutor.mergeSQLWithParams(targetQuery, resultSetMock)
            assert.equal(finalQuery, targetQuery)
        })
        it('should replace the SQL column placeholders with the values from the other resultset', () => {
            const targetQuery = `select distinct table_mock_alias.column_3 as "3" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1={1} and table_mock_alias.column_2={2}`
            const resultSetMock = {
                '1': 'COL--1',
                '2': 'COL--2',
                '3': -1,
                '4': 4,
                '5': 268,
                '6': 222,
                '7': 401,
                '8': 0,
                '9': 0,
                '10': -1
            }
            const finalQuery = queryExecutor.mergeSQLWithParams(targetQuery, resultSetMock)
            assert.equal(finalQuery, `select distinct table_mock_alias.column_3 as "3" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1='COL--1' and table_mock_alias.column_2='COL--2'`)
        })
    })
    describe('#buildTimeDistributedWhereClause()', () => {
        it('should build multiple WHERE clauses distributing the time column', () => {
            const arrTerms = [{
                leftTerm: 'table_mock_alias.time_column_1',
                operator: '>=',
                rightTerm: `'2018-04-26 11:00:00'`
            },
            { connector: 'and' },
            {
                leftTerm: 'table_mock_alias.time_column_1',
                operator: '<=',
                rightTerm: `'2018-04-26 11:40:00'`
            },
            { connector: 'and' },
            {
                leftTerm: 'table_mock_alias.column_7',
                operator: 'in',
                rightTerm: `('44')`
            },
            { connector: 'and' },
            {
                leftTerm: 'table_mock_alias.column_1',
                operator: '=',
                rightTerm: `'COL--1'`
            },
            { connector: 'and' },
            {
                leftTerm: 'table_mock_alias.column_2',
                operator: '=',
                rightTerm: `'COL--2'`
            },
            { connector: 'and' },
            {
                leftTerm: 'table_mock_alias.column_2',
                operator: '!=',
                rightTerm: `''`
            }]
            const whereClauses = queryExecutor.buildTimeDistributedWhereClause(arrTerms, 4)
            assert.equal(whereClauses.length, 4)
            assert.equal(whereClauses[0], `where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:09:59' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1 = 'COL--1' and table_mock_alias.column_2 = 'COL--2' and table_mock_alias.column_2 != ''`)
            assert.equal(whereClauses[1], `where table_mock_alias.time_column_1 >= '2018-04-26 11:10:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:19:59' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1 = 'COL--1' and table_mock_alias.column_2 = 'COL--2' and table_mock_alias.column_2 != ''`)
            assert.equal(whereClauses[2], `where table_mock_alias.time_column_1 >= '2018-04-26 11:20:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:29:59' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1 = 'COL--1' and table_mock_alias.column_2 = 'COL--2' and table_mock_alias.column_2 != ''`)
            assert.equal(whereClauses[3], `where table_mock_alias.time_column_1 >= '2018-04-26 11:30:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1 = 'COL--1' and table_mock_alias.column_2 = 'COL--2' and table_mock_alias.column_2 != ''`)            
        })
    })

    describe('#consolidateResultNonDistinct()', () => {
        it('should consolidate the resultset with the indicated aggregation on the projection terms', () => {
            const sql = `select table_mock_alias.column_1 as "1",table_mock_alias.column_2 as "2",-1 as "3",count(*) as "4",sum(table_mock_alias.column_4) as "5",max(table_mock_alias.column_4) as "6",sum(table_mock_alias.column_5) as "7",sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end) as "8",sum(case table_mock_alias.column_6 when 1 then 1 else 0 end) as "9",-1 as "10" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') group by table_mock_alias.column_1, table_mock_alias.column_2`
            const projectionTerms = [{
                    term: { expression: 'table_mock_alias.column_1', alias: '1' },
                    aggregationType: 'NONE',
                    distinct: false
                },
                {
                    term: { expression: 'table_mock_alias.column_2', alias: '2' },
                    aggregationType: 'NONE',
                    distinct: false
                },
                {
                    term: { expression: '-1', alias: '3' },
                    aggregationType: 'NONE',
                    distinct: false
                },
                {
                    term: { expression: 'count(*)', alias: '4' },
                    aggregationType: 'COUNT',
                    distinct: false
                },
                {
                    term: { expression: 'sum(table_mock_alias.column_4)', alias: '5' },
                    aggregationType: 'SUM',
                    distinct: false
                },
                {
                    term: { expression: 'max(table_mock_alias.column_4)', alias: '6' },
                    aggregationType: 'MAX',
                    distinct: false
                },
                {
                    term: { expression: 'sum(table_mock_alias.column_5)', alias: '7' },
                    aggregationType: 'SUM',
                    distinct: false
                },
                {
                    term:
                        {
                            expression: 'sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end)',
                            alias: '8'
                        },
                    aggregationType: 'SUM',
                    distinct: false
                },
                {
                    term:
                        {
                            expression: 'sum(case table_mock_alias.column_6 when 1 then 1 else 0 end)',
                            alias: '9'
                        },
                    aggregationType: 'SUM',
                    distinct: false
                },
                {
                    term: { expression: '-1', alias: '10' },
                    aggregationType: 'NONE',
                    distinct: false
                }]

            const groupByTerms = projectionTerms.filter(p => p.aggregationType == 'NONE' && p.term.expression != '-1')    
            const resultset = [{
                    '1': 'COL--1',
                    '2': 'COL--2',
                    '3': -1,
                    '4': 2,
                    '5': 24,
                    '6': 22,
                    '7': 36,
                    '8': 0,
                    '9': 0,
                    '10': -1
                },
                {
                    '1': 'COL--11',
                    '2': 'COL--22',
                    '3': -1,
                    '4': 1,
                    '5': 222,
                    '6': 222,
                    '7': 333,
                    '8': 0,
                    '9': 0,
                    '10': -1
                },
                {
                    '1': 'COL--1',
                    '2': 'COL--2',
                    '3': -1,
                    '4': 2,
                    '5': 244,
                    '6': 222,
                    '7': 365,
                    '8': 0,
                    '9': 0,
                    '10': -1
                },
                {
                    '1': 'COL--111',
                    '2': 'COL--222',
                    '3': -1,
                    '4': 1,
                    '5': 22,
                    '6': 22,
                    '7': 33,
                    '8': 0,
                    '9': 0,
                    '10': -1
                }]
            const res = queryExecutor.consolidateResultNonDistinct(resultset, projectionTerms, groupByTerms)

            assert.equal(res[0]['5'], '268')
            assert.equal(res[0]['6'], '222')
            assert.equal(res[0]['7'], '401')
        })
    })

    describe('#executeDistributed()', () => {
        it('should distribute the query execution and merge the resultsets, having repeated values still', async () => {
            const finalQuery = `select distinct table_mock_alias.column_3 as "3" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1='COL--1' and table_mock_alias.column_2='COL--2' order by "3" limit 11`
            const result = await queryExecutor.executeDistributed(finalQuery, 16)
            assert.equal(result.length, 4)
            assert.equal(result[0]['3'], 11)
            assert.equal(result[1]['3'], 113)
            assert.equal(result[2]['3'], 1)
            assert.equal(result[3]['3'], 11)
        })
        it('should distribute the query execution and merge the resultsets, dont having repeated values because coincidentally all the distinct values were on the same `batch`', async () => {
            const finalQuery = `select distinct table_mock_alias.column_3 as "3" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1='COL--1' and table_mock_alias.column_2='COL--2' order by "3" limit 11`
            const result = await queryExecutor.executeDistributed(finalQuery, 2)
            assert.equal(result.length, 3)
            assert.equal(result[0]['3'], 1)
            assert.equal(result[1]['3'], 11)
            assert.equal(result[2]['3'], 113)
        })

        it('should distribute the query execution and merge the resultsets, dont having repeated values because coincidentally all the distinct values were on the same `batch`', async () => {
            const queryWithGroupBy = `select table_mock_alias.column_1 as "1",table_mock_alias.column_2 as "2",-1 as "3",count(*) as "4",sum(table_mock_alias.column_4) as "5",max(table_mock_alias.column_4) as "6",sum(table_mock_alias.column_5) as "7",sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end) as "8",sum(case table_mock_alias.column_6 when 1 then 1 else 0 end) as "9",-1 as "10" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 23:40:00' and table_mock_alias.column_7 in ('44') group by table_mock_alias.column_1, table_mock_alias.column_2`
            const result = await queryExecutor.executeDistributed(queryWithGroupBy, 6)
            assert.equal(result[0]['5'], '268')
            assert.equal(result[0]['6'], '222')
            assert.equal(result[0]['7'], '401')
        })        
        
    })
    describe('#executeComposedQueries()', () => {
        it('should execute a series of queries and merge its result', async () => {
            const queries = [{
                sql: 'select table_mock_alias.column_1 as "1",table_mock_alias.column_2 as "2",-1 as "3",count(*) as "4",sum(table_mock_alias.column_4) as "5",max(table_mock_alias.column_4) as "6",sum(table_mock_alias.column_5) as "7",sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end) as "8",sum(case table_mock_alias.column_6 when 1 then 1 else 0 end) as "9",-1 as "10" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= \'2018-04-26 11:00:00\' and table_mock_alias.time_column_1 <= \'2018-04-26 23:40:00\' and table_mock_alias.column_7 in (\'44\') group by table_mock_alias.column_1, table_mock_alias.column_2',
                aggregationType: 'NONE',
                term: 'FULL',
                targetColumn: { expression: 'NONE', alias: 'NONE' },
                distinct: false,
                role: 'MASTER'
            },
            {
                term:
                    {
                        expression: 'count(distinct table_mock_alias.column_3)',
                        alias: '3'
                    },
                aggregationType: 'COUNT',
                distinct: true,
                sql: 'select distinct table_mock_alias.column_3 as "3" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= \'2018-04-26 11:00:00\' and table_mock_alias.time_column_1 <= \'2018-04-26 23:40:00\' and table_mock_alias.column_7 in (\'44\') and table_mock_alias.column_1={1} and table_mock_alias.column_2={2}',
                role: 'AGGREGATION',
                targetColumn:
                    {
                        expression: 'count(distinct table_mock_alias.column_3)',
                        alias: '3'
                    }
            },
            {
                term:
                    {
                        expression: 'count(distinct table_mock_alias.column_8)',
                        alias: '10'
                    },
                aggregationType: 'COUNT',
                distinct: true,
                sql: 'select distinct table_mock_alias.column_8 as "10" from table_mock as table_mock_alias where table_mock_alias.time_column_1 >= \'2018-04-26 11:00:00\' and table_mock_alias.time_column_1 <= \'2018-04-26 23:40:00\' and table_mock_alias.column_7 in (\'44\') and table_mock_alias.column_1={1} and table_mock_alias.column_2={2}',
                role: 'AGGREGATION',
                targetColumn:
                    {
                        expression: 'count(distinct table_mock_alias.column_8)',
                        alias: '10'
                    }
            }]
            
            const result = await queryExecutor.executeComposedQueries(queries)
            assert.equal(result[0]['1'], 'COL--1')
            assert.equal(result[0]['3'], 3)
            assert.equal(result[0]['5'], 268)
            assert.equal(result[0]['10'], 2)
            assert.equal(result[1]['1'], 'COL--111')
            assert.equal(result[1]['3'], 1)
            assert.equal(result[1]['5'], 22)
            assert.equal(result[1]['10'], 1)
            assert.equal(result[2]['1'], 'COL--11')
            assert.equal(result[2]['3'], 1)
            assert.equal(result[2]['5'], 222)
            assert.equal(result[2]['10'], 1)
        })        
    })
});
