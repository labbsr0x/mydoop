const log = require('debug')('query-splitter-log')
const debug = require('debug')('query-splitter-debug')
const parseUtils = require('./parse-utils')
const queryParser = require('./query-parser')

module.exports = {

    separateQueriesByDistinct: (query) => {
        query = parseUtils.normalizeQuery(query)
        debug('normalized-query::', query)

        const projectionTerms = queryParser.getProjectionTerms(query)
        debug('projectionTerms:: ', projectionTerms)

        const originalQuerySecondPart = `from ${query.split(' from ')[1]}`        
        debug('originalQuerySecondPart:: ', originalQuerySecondPart)
        
        let newQueries = [{ sql: "select ", aggregationType: "NONE", term: "FULL", targetColumn: { "expression": "NONE", "alias":"NONE"}, distinct: false, role: "MASTER"} ]
        
        projectionTerms.forEach(t => {
            //if the term has to consider DISTINCT values in aggregation, it should be made on a series of separated query that will:
            // -- retrieve the distinct values of the GROUP BY columns + the aggregated column
            // -- for each of those distinct values, it will have a query (can be parallelized) to fetch the distinct values and them count in memory
            if (t.distinct && t.aggregationType != 'NONE') { 
                newQueries[0].sql += `-1 as "${t.term.alias}",` //temporary -1 to be replaced when the derived aggregation finishes

                let secondPartGroupByArr = originalQuerySecondPart.split(' group by ')
                let secondPartWithoutGroupBy = secondPartGroupByArr[0]
                let andClauseWithGroupByTerms = ''
                if (secondPartGroupByArr[1]) { //identify the GROUP BY terms that will be distinct fetch separately to have the aggregation made in code
                    andClauseWithGroupByTerms = secondPartGroupByArr[1].split(' order by ')[0]
                                            .split(' limit ')[0]
                                            .split(' having ')[0]
                                            .split(',')
                                            .reduce((a, b, idx) => a + ` and ${b.trim()}={${queryParser.getAliasByColumnExpression(b, projectionTerms)}}`, '')                                            
                }
                const derivedQuery = parseUtils.normalizeQuery(`select ${t.term.expression.replace(/[()]/g, '')} as "${t.term.alias}" ${secondPartWithoutGroupBy} ${andClauseWithGroupByTerms}`.replace(t.aggregationType.toLowerCase(), ''))
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