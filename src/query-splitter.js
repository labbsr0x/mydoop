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

        const originalQuerySecondPart = `from ${query.split('from')[1]}`        
        debug('originalQuerySecondPart:: ', originalQuerySecondPart)
        
        let newQueries = [ {sql: "select ", type: "DIRECT"} ]
        
        projectionTerms.forEach(t => {
            if (t.distinct && t.aggregationType != 'NONE') { //if the term has to consider DISTINCT values in aggregation, it should be made on a separated query
                let secondPartWithoutGroupBy = originalQuerySecondPart.split('group')[0]
                const derivedQuery = parseUtils.normalizeQuery(`select ${t.term.replace(/[()]/g, '')} ${secondPartWithoutGroupBy}`.replace(t.aggregationType.toLowerCase(), ''))
                debug('derivedQuery', derivedQuery)
                newQueries.push({
                    sql: derivedQuery,
                    type: t.aggregationType.toUpperCase()
                })
                
            }else if (t.distinct && t.aggregationType == 'NONE') {
                throw new Error(`"DISTINCT" is only supported in aggregation functions, e.g.: select count(distinct column).`)

            }else{
                newQueries[0].sql += `${t.term},`

            }    
        })
        newQueries[0].sql = `${newQueries[0].sql.substring(0, newQueries[0].sql.length-1)} ${originalQuerySecondPart}`
        debug('newQueries::', newQueries)
        
        return newQueries
    } 

}