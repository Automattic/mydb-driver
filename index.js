
/**
 * Module dependencies.
 */

var monk = require('monk')
  , redis = require('redis')
  , debug = require('debug')('mydb-driver');

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
  monk.call(this, uri, opts, fn);

  opts = opts || {};

  // redis client
  var client = opts.redis;

  // if we got a host/port
  if (!client && false !== client) {
    var redisHost = opts.redisHost || '127.0.0.1';
    var redisPort = opts.redisPort || 6379;

    debug('initializing redis client to %s:%d', redisHost, redisPort);
    client = redis.createClient(opts.redisPort, opts.redisHost);
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
    this.collections[name] = new Collection(this, name);
    if (this.redis) {
      var self = this;
      this.collections[name].on('op', function(id, query, op){
        debug('publishing to redis %s channel', id);
        self.redis.publish(id, JSON.stringify([query, op]));
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

function Collection(manager, name){
  MonkCollection.call(this, manager, name);
};

/**
 * Inherits from `monk.Collection`.
 */

Collection.prototype.__proto__ = MonkCollection.prototype;

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
  promise.on('success', function () {
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

Collection.prototype.findAndModify = function(query, update, opts, fn){
  var promise = MonkCollection.prototype.findAndModify.call(this, query, update, opts, fn);
  if ('object' != typeof query.query && 'object' != typeof query.update) {
    query = { query: query, update: update };
  }
  if ('string' == typeof query.query || query.query.toHexString) {
    query.query = { _id: query.query };
  }
  var self = this;
  promise.on('success', function(doc){
    if (!doc) return;
    var id = doc._id.toString();
    self.emit('op', id, {}, query.update);
  });
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
