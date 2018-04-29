const log = require('debug')('query-parser-log')
const debug = require('debug')('query-parser-debug')
const parseUtils = require('./parse-utils')

module.exports = { 
    
    /**
     * Extract the projection terms and add the following attributes to qualify them:
     * - term: the string of the extracted term
     * - aggregationType: `NONE` | `SUM` | `COUNT` | `MAX` | `MIN` | `AVG`
     * - distinct: `true` | `false` > if the term should be `distinct`
     * 
     * @returns Array[] with the projection terms and its attributes
     */
    getProjectionTerms: (query) => {

        query = parseUtils.normalizeQuery(query)
        let spaces = query.split(' ')
        if (spaces[0].toLowerCase() == 'select' && spaces[1].toLowerCase() == 'distinct') {
            throw new Error(`"SELECT DISTINCT" is not yet supported. Only "SELECT" can be used for now.`)
        }

        log('query:: ', query)
        
        return parseUtils.extractProjectionTerms(query)
            .map(it => {
                let transformParams = {
                    term: it,
                    aggregationType: 'NONE',
                    distinct: false
                }
                if (it.toLowerCase().indexOf('sum(') != -1) {
                    transformParams.aggregationType = 'SUM'
                }
                if (it.toLowerCase().indexOf('count(') != -1) {
                    transformParams.aggregationType = 'COUNT'
                }
                if (it.toLowerCase().indexOf('max(') != -1) {
                    transformParams.aggregationType = 'MAX'
                }
                if (it.toLowerCase().indexOf('min(') != -1) {
                    transformParams.aggregationType = 'MIN'
                }
                if (it.toLowerCase().indexOf('avg(') != -1) {
                    transformParams.aggregationType = 'AVG'
                }

                if (it.toLowerCase().indexOf('distinct') != -1) {
                    transformParams.distinct = true
                }

                debug('transformParams:: ', transformParams)
                return transformParams
            })
    },

    getColumnName: (txt) => {
        return txt.replace(/(distinct|count|sum|avg|max|min|\(|\))/g, '').trim()        
    }

}