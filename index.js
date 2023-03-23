
/**
 * Module dependencies.
 */

const monk = require('monk');
const redis = require('redis');
const debug = require('debug')('mydb-driver');
const EventEmitter = require('events');

/**
 * Module exports.
 */

module.exports = exports = MyManager;
exports.__proto__ = monk;
exports.Collection = Collection;

/**
 * Monk `Collection` reference.
 */

var MonkCollection = monk.Collection;

/**
 * MyDB driver `Manager`.
 *
 * @param {Array|String} connection uri. replica sets can be an array or
 * comma-separated
 * @param {Object|Function} options or connect callback
 * @param {Function} connect callback
 * @api public
 */

function MyManager(uri, opts, fn){
  if (!(this instanceof MyManager)) return new MyManager(uri, opts, fn);

  opts = opts || {};

  const mongoOptions = {};
  // Filter out redis options so the mongo driver doesn't trip
  Object.keys( opts )
    .filter( ( key ) => -1 === [ 'redis', 'redisHost', 'redisPort' ].indexOf( key ) )
    .forEach( ( key ) => mongoOptions[ key ] = opts[ key ] );

  monk.call(this, uri, mongoOptions, fn);


  // redis client
  var client = opts.redis;

  // if we got a host/port
  if ( ! client && false !== client ) {
    var redisHost = opts.redisHost || '127.0.0.1';
    var redisPort = opts.redisPort || 6379;

    debug('initializing redis client to %s:%d', redisHost, redisPort);

    client = redis.createClient( { url: 'redis://' + redisHost + ':' + redisPort } );

    client.connect()
      .catch( ( e ) => debug( 'Redis connection failed %s', e ) )
  }

  this.redis = client;
};

/**
 * Inhetirs from `monk.Manager`
 */

MyManager.prototype.__proto__ = monk.prototype;

/**
 * Override monk collection creator.
 *
 * @param {String} name
 * @return {mydb.Collection}
 * @api public
 */

MyManager.prototype.get = function(name){
  if (!this.collections[name]) {
    this.collections[name] = new Collection( this, name, Object.assign({}, this._collectionOptions || {}) );
    if (this.redis) {
      var self = this;
      this.collections[name].on('op', function(id, query, op){
        debug('publishing to redis %s channel', id);
        self.redis.publish( id, JSON.stringify([query, op]) );
      });
    }
  }
  return this.collections[name];
};


/**
 * Monk collection.
 *
 * @param {monk.Manager} db manager
 * @param {String} name
 * @api public
 */

function Collection(manager, name, options) {
  MonkCollection.call( this, manager, name, options || { middlewares: null } );
};

/**
 * Inherits from `monk.Collection`.
 */

Collection.prototype.__proto__ = MonkCollection.prototype;

Collection.prototype.ensureEventEmitterExists = function() {
  if ( ! this._eventEmitter ) {
    this._eventEmitter = new EventEmitter();
  }
};

Collection.prototype.on = function() {
  this.ensureEventEmitterExists();
  return this._eventEmitter.on( ...arguments );
};

Collection.prototype.emit = function() {
  this.ensureEventEmitterExists();
  return this._eventEmitter.emit( ...arguments );
};

Collection.prototype.once = function() {
  this.ensureEventEmitterExists();
  return this._eventEmitter.once( ...arguments );
};

Collection.prototype.removeListener = function() {
  this.ensureEventEmitterExists();
  return this._eventEmitter.removeListener( ...arguments );
};

Collection.prototype.removeAllListeners = function() {
  this.ensureEventEmitterExists();
  return this._eventEmitter.removeAllListeners( ...arguments );
};

/**
 * Publishes an operation
 *
 * @param {Object} query that targets an _id
 * @param {Object} operation
 * @api private
 */

Collection.prototype.pub = function(query, op, promise){
  var self = this;
  debug('waiting on update success to emit op');
  promise.then(function () {
    debug('emitting op %j for query %j', op, query);
    var id = query._id.toString();
    delete query._id;
    self.emit('op', id, query, op);
  });
};

/**
 * Overrides `findAndModify`.
 *
 * @param {Object} search query, or { query, update } object
 * @param {Object} optional, update object
 * @param {Object|String|Array} optional, options or fields
 * @param {Function} callback
 * @return {Promise}
 * @api public
 */

Collection.prototype.findAndModify = function( query, update, opts, fn ) {
  const queryObject = {}

  if ( 'object' === typeof query.query ) {
    queryObject.query = query.query;
  } else if ( 'string' === typeof query.query || query.query?.toHexString ) {
    queryObject.query = { _id: query.query };
  } else {
    queryObject.query = query;
  }

  if ( 'object' === typeof query.update ) {
    queryObject.update = query.update;
  } else {
    queryObject.update = update;
  }

  var promise = MonkCollection.prototype.findOneAndUpdate.call(this, queryObject.query, queryObject.update, opts, fn);
  var self = this;
  promise.then( function( doc ) {
    if (!doc) return;
    var id = doc._id.toString();
    self.emit('op', id, {}, queryObject.update);
  } );
  return promise;
};

/**
 * Overrides `update`
 *
 * @param {String|Object} object id or search query
 * @param {Object} update obj
 * @param {Object|String|Array} optional, options or fields
 * @param {Function} callback
 * @return {Promise}
 * @api public
 */

var oldUpdate = MonkCollection.prototype.update;

Collection.prototype.update = function(search, update, opts, fn){
  if ('string' == typeof search || 'function' == typeof search.toHexString) {
    return this.update({ _id: search }, update, opts, fn);
  }

  var self = this;
  var promise;

  if ('function' == typeof opts) {
    fn = opts;
    opts = {};
  }

  if (search._id || opts.multi) {
    promise = oldUpdate.call(this, search, update, opts, fn);
    if (search._id) this.pub(search, update, promise);
  } else if (!opts.multi) {
    // override some options
    opts.safe = true;
    opts.fields = { _id: 1 };
    promise = this.findAndModify(search, update, opts, fn);
  }

  return promise;
};
