const assert = require('assert');
const queryParser = require('../src/query-parser')

describe('queryParser', () => {
    describe('#getProjectionTerms()', () => {
        let query = `SELECT
                        table_mock_alias.column_1 as "1", 
                        table_mock_alias.column_2 as "2", 
                        count(distinct table_mock_alias.column_3) as "3", 
                        count(*) as "4", 
                        sum(table_mock_alias.column_4) as "5", 
                        max(table_mock_alias.column_4) as "6", 
                        sum(table_mock_alias.column_5) as "7", 
                        sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end) as "8", 
                        sum(case table_mock_alias.column_6 when 1 then 1 else 0 end) as "9", 
                        count(distinct table_mock_alias.column_8) as "10"
                        FROM
                        table_mock AS table_mock_alias
                        WHERE
                        table_mock_alias.time_column_1 >= '2018-04-26 11:00:00'
                        AND table_mock_alias.time_column_1 <= '2018-04-26 23:40:00'
                        AND table_mock_alias.column_7 IN ('44') 
                        GROUP BY table_mock_alias.column_1, table_mock_alias.column_2
                    `

        let result = queryParser.getProjectionTerms(query)

        it('should return NONE to non aggregation terms', () => {
            assert.equal(result[0].aggregationType, 'NONE')
            assert.equal(result[1].aggregationType, 'NONE')
        });
        it('should return COUNT to COUNT aggregation terms', () => {
            assert.equal(result[2].aggregationType, 'COUNT')
            assert.equal(result[3].aggregationType, 'COUNT')
            assert.equal(result[9].aggregationType, 'COUNT')
        });
        it('should return SUM to SUM aggregation terms', () => {
            assert.equal(result[4].aggregationType, 'SUM')
            assert.equal(result[6].aggregationType, 'SUM')
            assert.equal(result[7].aggregationType, 'SUM')
            assert.equal(result[8].aggregationType, 'SUM')
        });
        it('should return MAX to MAX aggregation terms', () => {
            assert.equal(result[5].aggregationType, 'MAX')
        });

        it('should return DISTINCT to distinct terms', () => {
            assert.equal(result[0].distinct, false)
            assert.equal(result[1].distinct, false)
            assert.equal(result[2].distinct, true)
            assert.equal(result[3].distinct, false)
            assert.equal(result[4].distinct, false)
            assert.equal(result[5].distinct, false)
            assert.equal(result[6].distinct, false)
            assert.equal(result[7].distinct, false)
            assert.equal(result[8].distinct, false)
            assert.equal(result[9].distinct, true)
        });

        
        it('should THROW EXCEPTION beacuse it is not yet supported', () => {
            query = `SELECT DISTINCT * FROM TABLE_MOCK`
            assert.throws(function() {queryParser.getProjectionTerms(query)})
        });

    });

    describe('#getAliasByColumnExpression()', () => {
        it('should find the column alias by the projection expression', () => {
            const mockTerms = [{
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
                term: { expression: 'count(distinct table_mock_alias.column_3)', alias: '3' },
                aggregationType: 'COUNT',
                distinct: true
            },
            {
                term: { expression: 'count(*)', alias: '4' },
                aggregationType: 'COUNT',
                distinct: false
            },
            {
                term: { expression: 'max(table_mock_alias.column_4)', alias: '6' },
                aggregationType: 'MAX',
                distinct: false
            },
            {
                term: { expression: 'sum(case table_mock_alias.column_4 when 1 then 1 when 0 then 1 else 0 end)', alias: '8' },
                aggregationType: 'SUM',
                distinct: false
            }]
            
            mockTerms.forEach(t => {
                assert.equal(queryParser.getAliasByColumnExpression(t.term.expression, mockTerms), t.term.alias)
            })
        });
    })
});
