'use strict'

const fs = require('fs')
const path = require('path')
const request = require('request')
const {isEmpty} = require('ramda')
const stiq = require('commander')
const pkg = require('./package.json')

const handleResponse = (err, res, body) => {
  if (stiq.json) console.log(JSON.stringify(err || body))
  else {
    if (err) throw err
    console.log(body)
  }
}

const fullUrl = (endpoint = '') => {
  const {index, type} = stiq
  let hostAndPort = `${stiq.host}:${stiq.port}`
  return 'http://' + path.join(hostAndPort, index || '', type || '', endpoint)
}

stiq
  .version(pkg.version)
  .description(pkg.description)
  .usage('[options] <command> [...]')
  .option('-o, --host <hostname>', 'hostname [localhost]', 'localhost')
  .option('-p, --port <number>', 'port number [9200]', '9200')
  .option('-j, --json', 'format output as JSON')
  .option('-i, --index <name>', 'which index to use')
  .option('-t, --type <type>', 'default type for bulk operation')
  .option('-f, --filter <type>', 'source filter for query results')

stiq
  .command('url [path]')
  .description('generate the URL for the options and path (default is /)')
  .action((path = '/') => console.log(fullUrl(path)))

stiq
  .command('get [path]')
  .description('perform an HTTP GET request for path (default is /)')
  .action((path = '/') => {
    const options = {
      url: fullUrl(path),
      json: stiq.json
    }
    request(options, handleResponse)
  })

stiq
  .command('create-index')
  .description('create an index')
  .action(() => {
    if (!stiq.index) {
      const msg = 'No index specified! Use --index <name>'
      if (!stiq.json) throw Error(msg)
      console.log(JSON.stringify({error: msg}))
    } else {
      request.put(fullUrl(), handleResponse)
    }
  })

stiq
  .command('list-indices')
  .alias('li')
  .description('get a list of indices in this cluster')
  .action(() => {
    const endpoint = stiq.json ? '_all' : '_cat/indices?v'
    const options = {
      url: fullUrl(endpoint),
      json: stiq.json
    }
    request(options, handleResponse)
  })

stiq
  .command('bulk <file>')
  .description('read and perform bulk options from the specified file')
  .action(file => {
      fs.stat(file, (err, stats) => {
          if (err) {
              if (stiq.json) console.log(JSON.stringify(err))
              else throw err
          }

          const options = {
              url: fullUrl('_bulk'),
              json: true,
              headers: {
                  'content-lemgth': stats.size,
                  'content-type': 'application/json'
              }
          };

          const req = request.post(options)
          const stream = fs.createReadStream(file)
          stream.pipe(req)
          req.pipe(process.stdout)
      })
  })

stiq
  .command('query [queries...]')
  .alias('q')
  .description('perform an Elasticsearch query')
  .action((queries = []) => {
    const options = {
      url: fullUrl('_search'),
      json: stiq.json,
      qs: {}
    }

    if (queries && queries.length) options.qs.q = queries.join(' ')
    if (stiq.filter) options.qs._source = stiq.filter

    request(options, handleResponse)
  })

stiq.parse(process.argv)

if (isEmpty(stiq.args)) {
  stiq.help()
}
