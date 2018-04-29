const assert = require('assert');
const queryExecutor = require('../src/query-executor')

describe('queryExecutor', () => {

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
    describe('#buildTimeDistributedWhereClause()', () => {
        it('should build multiple WHERE clauses distributing the time column', () => {
            const arrTerms = [{
                leftTerm: 'table_mock_alias.time_column_1',
                operator: '>=',
                rightTerm: '\'2018-04-26 11:00:00\''
            },
            { connector: 'and' },
            {
                leftTerm: 'table_mock_alias.time_column_1',
                operator: '<=',
                rightTerm: '\'2018-04-26 11:40:00\''
            },
            { connector: 'and' },
            {
                leftTerm: 'table_mock_alias.column_7',
                operator: 'in',
                rightTerm: '(\'44\')'
            },
            { connector: 'and' },
            {
                leftTerm: 'table_mock_alias.column_1',
                operator: '=',
                rightTerm: '\'COL--1\''
            },
            { connector: 'and' },
            {
                leftTerm: 'table_mock_alias.column_2',
                operator: '=',
                rightTerm: '\'COL--2\''
            }]
            const whereClauses = queryExecutor.buildTimeDistributedWhereClause(arrTerms, 4)
            assert.equal(whereClauses.length, 4)
            assert.equal(whereClauses[0], `where table_mock_alias.time_column_1 >= '2018-04-26 11:00:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:09:59' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1 = 'COL--1' and table_mock_alias.column_2 = 'COL--2'`)
            assert.equal(whereClauses[1], `where table_mock_alias.time_column_1 >= '2018-04-26 11:10:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:19:59' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1 = 'COL--1' and table_mock_alias.column_2 = 'COL--2'`)
            assert.equal(whereClauses[2], `where table_mock_alias.time_column_1 >= '2018-04-26 11:20:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:29:59' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1 = 'COL--1' and table_mock_alias.column_2 = 'COL--2'`)
            assert.equal(whereClauses[3], `where table_mock_alias.time_column_1 >= '2018-04-26 11:30:00' and table_mock_alias.time_column_1 <= '2018-04-26 11:40:00' and table_mock_alias.column_7 in ('44') and table_mock_alias.column_1 = 'COL--1' and table_mock_alias.column_2 = 'COL--2'`)            
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
        
    })
});
