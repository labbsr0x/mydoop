const log = require('debug')('query-splitter-log')
const debug = require('debug')('query-splitter-debug')
const parseUtils = require('./parse-utils')
const queryParser = require('./query-parser')

module.exports = {

    /**
     * Takes a SQL, analyzes its projections terms ans segregate the terms that has `aggregation` function using `distinct`
     * keywork into separated queries
     * 
     * @param sql Query that will have its distinct aggregation terms run separatedely
     */
    separateQueriesByDistinct: (sql) => {
        sql = parseUtils.normalizeQuery(sql)
        debug('normalized-sql::', sql)

        const projectionTerms = queryParser.getProjectionTerms(sql)
        debug('projectionTerms:: ', projectionTerms)

        const originalQuerySecondPart = `from ${sql.split(' from ')[1]}`        
        debug('originalQuerySecondPart:: ', originalQuerySecondPart)
        
        let newQueries = [{ sql: "select ", aggregationType: "NONE", term: "FULL", targetColumn: { "expression": "NONE", "alias":"NONE"}, distinct: false, role: "MASTER"} ]
        
        projectionTerms.forEach(t => {
            //if the term has to consider DISTINCT values in aggregation, it should be made on a series of separated sql that will:
            // -- retrieve the distinct values of the GROUP BY columns + the aggregated column
            // -- for each of those distinct values, it will have a sql (can be parallelized) to fetch the distinct values and them count in memory
            if (t.distinct && t.aggregationType != 'NONE') { 
                newQueries[0].sql += `-1 as "${t.term.alias}",` //temporary -1 to be replaced when the derived aggregation finishes

                let secondPartGroupByArr = originalQuerySecondPart.split(' group by ')
                let secondPartWithoutGroupBy = secondPartGroupByArr[0]
                let andClauseWithoutGroupByTerms = ''
                if (secondPartGroupByArr[1]) { //identify the GROUP BY terms that will be distinct fetch separately to have the aggregation made in code
                    andClauseWithoutGroupByTerms = secondPartGroupByArr[1].split(' order by ')[0]
                                            .split(' group by ')[0] 
                                            .split(' limit ')[0]
                                            .split(' having ')[0]
                                            .split(',')
                                            .reduce((a, b, idx) => a + ` and ${b.trim()}={${queryParser.getAliasByColumnExpression(b, projectionTerms)}}`, '')                                            
                }
                const derivedQuery = parseUtils.normalizeQuery(`select ${t.term.expression.replace(/[()]/g, '')} as "${t.term.alias}" ${secondPartWithoutGroupBy} ${andClauseWithoutGroupByTerms}`.replace(t.aggregationType.toLowerCase(), ''))
                debug('derivedQuery', derivedQuery)
                newQueries.push(Object.assign({}, t, { sql: derivedQuery, role: "AGGREGATION", targetColumn: t.term }) )
                
            }else if (t.distinct && t.aggregationType == 'NONE') {
                throw new Error(`"DISTINCT" is only supported in aggregation functions, e.g.: select count(distinct column).`)

            }else{
                newQueries[0].sql += `${t.term.expression} as "${t.term.alias}",`

            }    
        })
        newQueries[0].sql = `${newQueries[0].sql.substring(0, newQueries[0].sql.length-1)} ${originalQuerySecondPart}`
        debug('newQueries::', newQueries)
        
        return newQueries
    } 

}