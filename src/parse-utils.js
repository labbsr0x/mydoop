const log = require('debug')('parse-utils-log')
const debug = require('debug')('parse-utils-debug')

module.exports = {

    isKeyWord: function(str) {
        return !!str.match(/(and\s*|or\s*)/g)
    },

    isOperator: function (str) {
        return !!str.match(/(>=\s*|<=\s*|>\s*|<\s*|<>\s*|=\s*|!=\s*|between\s*|like\s*|in\s*)/g)
    },

    normalizeQuery: function(query) {
        return query.replace(/\s\s+/g, ' ').replace(/\n/g, '').toLowerCase().trim()
    },
    /**
     * Extract all the terms from projection and returns it as an array
     * @returns Array [] with the projections terms
     */
    extractProjectionTerms: function(query) {
        query = this.normalizeQuery(query)
        const res = query.substring(query.indexOf('select') + 'select'.length, query.indexOf('from'))
                              .split(',')
                              .map(p => p.split(' as '))
                              .map(x => { return { "expression": x[0].trim(), "alias": (x[1] ? x[1].trim().replace(/[\"|\`]/g, '') : '') } })
        debug('res:::', res)
        return res                              
    }
}