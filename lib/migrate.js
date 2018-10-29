'use strict'

const EventEmitter = require('events')
const Fs = require('fs')
const Joi = require('joi')
const Mask = require('json-mask')
const Moment = require('moment')
const Path = require('path')

const internals = {}

const Migrate = function (opt) {
  emit('info', 'Validating options', opt)()
  return validateOptions(opt)
    .then(emit('info', 'Connecting to RethinkDB', opt))
    .then(connectToRethink)
    .then(createDbIfInexistent)
    .then(emit('info', 'Executing Migrations', opt))
    .then(executeMigration)
    .then(emit('info', 'Closing connection', opt))
    .then(closeConnection)
}

internals.emitter = new EventEmitter()

Migrate.emitter = internals.emitter

module.exports = Migrate

function validateOptions (options) {
  const schema = Joi.object().keys({
    op: Joi.string().valid('up', 'down').required()
      .description('Migration command'),
    to: Joi.string()
      .description('An optional migration name to run to'),
    driver: Joi.string().valid('rethinkdb', 'rethinkdbdash').default('rethinkdb')
      .description('Rethinkdb javascript driver'),
    migrationsTable: Joi.string().default('_migrations')
      .description('Table where meta information about migrations will be saved'),
    ignoreTimestamp: Joi.boolean().default(0)
      .description('Ignore timestamp when applying migrations'),
    migrationsDirectory: Joi.string().default('migrations')
      .description('Directory where migration files will be saved'),
    additionalMigrationsDirectories: Joi.array().items(Joi.string())
      .description('Additional directories where migration files can be read from'),
    relativeTo: Joi.string().default(process.cwd())
      .description('Root path from which migration directory will be searched'),
    host: Joi.string().default('localhost')
      .description('The host to connect to, if using rethinkdb official driver'),
    port: Joi.number().default(28015)
      .description('The port to connect on, if using rethinkdb official driver'),
    db: Joi.string().required().description('Database name'),
    user: Joi.string().description('Rethinkdb user'),
    username: Joi.string().description('Rethinkdb username'),
    password: Joi.string().description('Rethinkdb password'),
    authKey: Joi.string().description('Rethinkdb authkey'),
    discovery: Joi.any().when('driver', { is: 'rethinkdb', then: Joi.any().forbidden(), otherwise: Joi.boolean() })
      .description('Whether or not the driver should try to keep a list of updated hosts'),
    pool: Joi.any().when('driver', { is: 'rethinkdb', then: Joi.any().forbidden(), otherwise: Joi.boolean().default(false) })
      .description('Whether or not to use a connection pool'),
    cursor: Joi.any().when('driver', { is: 'rethinkdb', then: Joi.any().forbidden(), otherwise: Joi.boolean().default(true) })
      .description('If true, cursors will not be automatically converted to arrays when using rethinkdbdash'),
    servers: Joi.any().when('driver', {
      is: 'rethinkdb',
      then: Joi.any().forbidden(),
      otherwise: Joi.array().items(Joi.object().keys({
        host: Joi.string()
          .description('The host to connect to'),
        port: Joi.number().default(28015)
          .description('The port to connect on')
      }))
    }),
    ssl: Joi.alternatives().try(Joi.object(), Joi.boolean()).default(false).description('Rethinkdb SSL/TLS support'),
    emitter: Joi.any().description('An alternate event emitter on which to emit event'),
    r: Joi.any().description('An r rethinkdb driver object provided directly'),
    dontCloseConnectionAfterMigrations: Joi.boolean().description('Used in combination with specifying r typically. In this case this module will not close the connection when finished.')
  }).without('user', 'username').without('password', 'authKey').required()

  return new Promise((resolve, reject) => {
    Joi.validate(options, schema, (err, validated) => {
      if (err) {
        return reject(err)
      }

      resolve(validated)
    })
  })
}

let hasAlreadyWaitedForReadyForWritesOnR = [];
function wait (options) {
  if (options.driver === 'rethinkdb') {
    return Promise.resolve(options)
  }

  const { r, conn, db } = options

  return r.dbList().run(conn)
    .then(toArray)
    .then(list => {
      if (list.indexOf(db) !== -1 && hasAlreadyWaitedForReadyForWritesOnR.indexOf(r) === -1) {
        return r
          .db(options.db).wait([
            { waitFor: 'ready_for_writes', timeout: 20 }
          ])
          .run(conn)
          .then(() => {
            hasAlreadyWaitedForReadyForWritesOnR.push(r);
            return options
          })
      }
      return Promise.resolve(options)
    })
}

function connectToRethink (options) {
  const r = selectDriver(options)

  if (options.r) {
    return Promise.resolve(Object.assign({}, options));
  }

  if (options.driver === 'rethinkdbdash' && options.servers && options.pool) {
    return Promise.resolve(Object.assign({}, options, { r }))
  }

  if (options.host && options.port) {
    return r.connect(Mask(options, 'db,host,port,user,username,password,authKey,ssl'))
      .then(conn => {
        return Object.assign({}, options, { r, conn })
      })
  }
}

function selectDriver (options) {
  if (options.r) {
    return options.r;
  }
  if (options.driver === 'rethinkdb') {
    return require('rethinkdb')
  }
  return require('rethinkdbdash')(Mask(options, 'db,user,host,port,username,password,authKey,discovery,pool,cursor,servers,ssl'))
}

function createDbIfInexistent (options) {
  const { r, conn, db } = options

  return r.dbList().run(conn)
    .then(toArray)
    .then(list => {
      if (list.indexOf(db) < 0) {
        emit('info', 'Creating db' + db, options)()
        return r.dbCreate(db).run(conn)
      }
    })
    .then(() => {
      if (options.driver === 'rethinkdb' || !options.pool) {
        conn.use(db)
      }
      return options
    })
    .then(wait)
}

function toArray (cursor) {
  if (Array.isArray(cursor)) {
    return Promise.resolve(cursor)
  }

  return cursor.toArray()
}

function executeMigration (options) {
  const proxyTable = {
    up: migrateUp,
    down: migrateDown
  }

  return proxyTable[options.op](options)
}

function migrateUp (options) {
  return getLatestMigrationExecuted(options)
    .then(latest => getUnExecutedMigrations(latest, options))
    .then(newerMigrations => runMigrations('up', newerMigrations, options))
    .then(emit('info', 'Saving metada', options))
    .then(executedMigrations => saveExecutedMigrationsMetadata(executedMigrations, options))
    .then(() => options)
}

function migrateDown (options) {
  return getExecutedMigrations(options)
    .then(migrations => runMigrations('down', migrations, options))
    .then(migrations => clearMigrationsTable(migrations, options))
    .then(emit('info', 'Cleared migrations table', options))
    .then(() => options)
}

function getLatestMigrationExecuted (options) {
  return getAllMigrationsExecuted(options)
    .then(migrations => {
      if (!migrations.length) {
        return {
          timestamp: Moment().year(1900)
        }
      }
      return migrations[0]
    })
}

function ensureMigrationsTable (options) {
  const { r, conn, migrationsTable } = options

  return r.tableList().run(conn)
    .then(toArray)
    .then(list => {
      if (list.indexOf(migrationsTable) < 0) {
        return r.tableCreate(migrationsTable).run(conn)
          .then(() => r.table(migrationsTable).indexCreate('timestamp').run(conn))
          .then(() => r.table(migrationsTable).indexWait().run(conn))
      }
    })
}

function getAllMigrationsExecuted (options) {
  const { r, conn, migrationsTable } = options

  return ensureMigrationsTable(options)
    .then(() => r.table(migrationsTable)
      .orderBy({ index: r.desc('timestamp') })
      .run(conn)
      .then(toArray)
    )
    .then(migrations => migrations.map(migration => Object.assign({}, migration, {
      timestamp: Moment.utc(migration.timestamp)
    })))
}

function getMigrationsFromPath (options) {
  const { migrationsDirectory, additionalMigrationsDirectories, relativeTo } = options
  const path = Path.resolve(relativeTo, migrationsDirectory)
  const migrationRegExp = /^(\d{14})-(.*)\.js$/

  let additionalPaths = []
  if (additionalMigrationsDirectories) {
    additionalPaths = additionalMigrationsDirectories.map(directory => Path.resolve(relativeTo, directory))
  }

  return readMigrationFilenamesFromPaths([path, ...additionalPaths])
    .then(files => [].concat(...files).filter(file => file.base.match(migrationRegExp)))
    .then(migrationFiles => migrationFiles.map(file => {
      const [timestamp, name] = file.base.match(migrationRegExp)

      return {
        timestamp: Moment.utc(timestamp, 'YYYYMMDDHHmmss'),
        name: name,
        filename: file.base,
        dir: file.dir
      }
    }))
}

function getExecutedMigrations (options) {
  return getMigrationsFromPath(options)
    .then(migrations => filterMigrationsUntilLimit(migrations, options, 'down'))
    .then(migrations => sortMigrations(migrations, true))
    .then(migrations => loadMigrationsCode(migrations, options))
}

function getUnExecutedMigrations (latestExecutedMigration, options) {
  return getMigrationsFromPath(options)
    .then(migrations => filterMigrationsOlderThan(migrations,
      latestExecutedMigration.timestamp, options))
    .then(olderMigrations => filterMigrationsUntilLimit(olderMigrations, options, 'up'))
    .then(sortMigrations)
    .then(migrations => loadMigrationsCode(migrations, options))
}

function readMigrationFilenamesFromPaths (paths) {
  return Promise.all(paths.map(path => {
    return new Promise((resolve, reject) => {
      Fs.readdir(path, (err, files) => {
        if (err) {
          return reject(err)
        }
        resolve(files.map(f => Path.parse(Path.join(path, f))))
      })
    })
  }))
}

function filterMigrationsOlderThan (migrations, reference, options) {
  if (!options.ignoreTimestamp) {
    return migrations.filter(migration => migration.timestamp.isAfter(Moment(reference)))
  }
  return migrations
}

function filterMigrationsUntilLimit (migrations, options, direction) {
  if (options.to) {
    let migrationLimit
    if (direction === 'up') {
      migrationLimit = migrations.slice().reverse().find(migration => migration.name === options.to)
    } else {
      migrationLimit = migrations.find(migration => migration.name === options.to)
    }
    if (!migrationLimit) {
      // No migration found so do not run any, or should we error?
      return []
    }
    return migrations.filter(migration => direction === 'up' ? migration.timestamp.isSameOrBefore(migrationLimit.timestamp) : migration.timestamp.isSameOrAfter(migrationLimit.timestamp))
  }
  return migrations
}

function loadMigrationsCode (migrations, options) {
  return migrations.map(migration => Object.assign({}, migration, { code: require(Path.resolve(migration.dir, migration.filename)) }))
}

function sortMigrations (migrations, orderDesc = false) {
  return migrations.sort((a, b) => {
    if (a.timestamp.isBefore(b.timestamp)) {
      return orderDesc ? 1 : -1
    } else if (b.timestamp.isBefore(a.timestamp)) {
      return orderDesc ? -1 : 1
    }
    return 0
  })
}

function runMigrations (direction, migrations, options) {
  const { r, conn } = options
  return migrations.reduce(
    (chain, migration) => chain.then(() => migration.code[direction](r, conn)
      .then(emit('info', `Executed migration ${migration.name} ${options.op} (file: ${migration.filename}, dir:${Path.relative(options.relativeTo, migration.dir)} )`, options))),
    Promise.resolve()
  ).then(() => migrations)
}

function saveExecutedMigrationsMetadata (migrations, options) {
  const { r, conn, migrationsTable } = options

  return migrations
    .map(migration => ({ timestamp: migration.timestamp.toISOString(), name: migration.name, filename: migration.filename, dir: Path.relative(options.relativeTo, migration.dir) }))
    .reduce((chain, migration) => chain.then(() => r.table(migrationsTable).insert(migration).run(conn)), Promise.resolve())
}

function clearMigrationsTable (migrations, options) {
  const { r, conn, migrationsTable } = options

  return Promise.all(
    migrations.map(
      item => r.table(migrationsTable)
        .filter({ filename: item.filename })
        .delete()
        .run(conn)
    )
  )
}

function closeConnection (options) {
  const { r, conn } = options

  if (options.dontCloseConnectionAfterMigrations) {
    return;
  }

  if (options.driver === 'rethinkdbdash' && options.pool) {
    return r.getPoolMaster().drain()
      .then(() => {
        if (!options.pool) {
          return conn.close()
        }
      })
  }

  return conn.close()
}

function emit (name, data, options) {
  return function (arg) {
    internals.emitter.emit(name, data)
    if (options && options.emitter) {
      options.emitter.emit(name, data);
    }
    return arg
  }
}
