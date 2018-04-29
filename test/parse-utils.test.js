const assert = require('assert');
const parseUtils = require('../src/parse-utils')

describe('parseUtils', () => {
    describe('#extractProjectionTerms()', () => {
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
        it('should return the projection terms as an Array', () => {
            const terms = parseUtils.extractProjectionTerms(query)
            assert.equal(terms.length, 10)            
        });
        it('should return each projection term as an object with column name and alias', () => {
            const terms = parseUtils.extractProjectionTerms(query)
            assert.equal(terms[0].expression, 'table_mock_alias.column_1')
            assert.equal(terms[1].expression, 'table_mock_alias.column_2')
            assert.equal(terms[2].expression, 'count(distinct table_mock_alias.column_3)')
            assert.equal(terms[3].expression, 'count(*)')

            assert.equal(terms[0].alias, "1")
            assert.equal(terms[1].alias, "2")
            assert.equal(terms[2].alias, "3")
            assert.equal(terms[3].alias, "4")
        });
    });
});