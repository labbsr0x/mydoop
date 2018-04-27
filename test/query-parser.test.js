const assert = require('assert');
const queryParser = require('../src/query-parser')

describe('queryParser', () => {
    describe('#getProjectionTerms()', () => {
        let query = `SELECT
                        TABLE_MOCK_alias.column_1 , 
                        TABLE_MOCK_alias.column_2 , 
                        count(distinct TABLE_MOCK_alias.column_3) , 
                        count(*) , 
                        sum(TABLE_MOCK_alias.column_4) , 
                        max(TABLE_MOCK_alias.column_4) , 
                        sum(TABLE_MOCK_alias.column_5) , 
                        sum(case TABLE_MOCK_alias.column_4 when 1 then 1 when 0 then 1 else 0 end) , 
                        sum(case TABLE_MOCK_alias.column_6 when 1 then 1 else 0 end) , 
                        count(distinct TABLE_MOCK_alias.column_7) 
                        FROM
                        TABLE_MOCK AS TABLE_MOCK_alias
                        WHERE
                        TABLE_MOCK_alias.time_column_1 >= ''2018-04-26 11:00:00''
                        AND TABLE_MOCK_alias.time_column_1 <= '2018-04-26 11:10:00'
                        AND TABLE_MOCK_alias.column_7 IN ('44') 
                        GROUP BY TABLE_MOCK_alias.column_1, TABLE_MOCK_alias.column_2
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
});
