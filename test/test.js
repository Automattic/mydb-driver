
var expect = require('expect.js');
var mydb = require('..');
var monk = require('monk');
var redis = require('redis');

describe('mydb-driver', function(){

  describe('`op` event', function(){
    var db = mydb('localhost:31003/mydb-driver-test', { redis: false });
    var users = db.get('users-' + Date.now());

    it('should export extended `Collection`', function(){
      expect(users).to.be.a(mydb.Collection);
      expect(mydb.Collection).to.not.be(monk.Collection);
    });

    it('should export monk exports', function(){
      expect(mydb.cast).to.be.a('function');
    });

    it('should work with updates', function( done ){
      users
        .insert( {} )
        .then( ( user ) => {
          const callback = function(id, query, op) {
            expect(id).to.equal(user._id.toString());
            expect(query).to.eql({});
            expect(op).to.eql({ $set: {} });
            done();
          };

          users.once( 'op', callback );
          users
          .update({ _id: user._id }, { $set: {} })
          .catch( ( e ) => {
            users.removeListener( 'op', callback );
            done( e );
          } );
        } )
        .catch( ( e ) => done( e ) );
    });

    it('should not be emitted when errors occur', function(done){
      var col = db.get('users-' + Date.now());
      col
        .insert({ a: [1, 2] })
        .then( ( user ) => {
          const invalidFn = function(){
            users.removeListener( 'op', invalidFn );
            done(new Error('Invalid test'));
          };
  
          col.once( 'op', invalidFn );

          col.update({ _id: user._id }, { $pull: { a: 1 }, $push: { a: 3 } })
          .then( () => {
            col.removeListener( 'op', invalidFn );
            done();
          } )
          .catch( ( e ) => {
            col.removeListener( 'op', invalidFn );
            done();
          });
        } )
        .catch( ( e ) => done( e ) );
    });

    it('should work with updates and positional op', function(done) {
      users
        .insert( { test : [ { a: 'b' } ] } )
        .then( ( user ) => {
          const callback = function(id, query, op){
            expect(id).to.be(user._id.toString());
            expect(query).to.eql({ 'test.a': 'b' });
            expect(op).to.eql({ $set : { 'test.$.a': 'c' } });
            done();
          };

          users.once( 'op', callback );
  
          users
            .update({ _id: user._id, 'test.a': 'b' }, { $set : { 'test.$.a': 'c' } })
            .catch( ( e ) => {
              users.removeListener( 'op', callback );
              done( e );
            } );
        } )
        .catch( ( e ) => done( e ) );
    });

    it('should work with updates (shorthand)', function(done){
      users
        .insert({})
        .then( ( user ) => {
          const callback = function(id, query, op){
            expect(id).to.be(user._id.toString());
            expect(query).to.eql({});
            expect(op).to.eql({ $pull: { a: 'woot' } });
            done();
          };

          users.once( 'op', callback );

          users
            .update(user._id, { $pull: { a: 'woot' } })
            .catch( ( e ) => {
              users.removeListener( 'op', callback );
              done( e );
            } );
        } )
        .catch( ( e ) => done( e ) );
    });

    it('should work with updates (shorthand#2)', function(done){
      users
      .insert({})
      .then( ( user ) => {
        const callback = function(id, query, op){
          expect(id).to.be(user._id.toString());
          expect(query).to.eql({});
          expect(op).to.eql({ $pull: { a: 'woot' } });
          done();
        };

        users.once( 'op', callback );

        users
          .update(user._id.toString(), { $pull: { a: 'woot' } })
          .catch( ( e ) => {
            users.removeListener( 'op', callback );
            done( e );
          } );
      })
    });

    it('should work with findAndModify', function(done){
      users
        .insert( {} )
        .then( ( user ) => {
          const callback = function(id, query, op){
            expect(id).to.be(user._id.toString());
            expect(query).to.eql({});
            expect(op).to.eql({ $pull: { a: 'woot' } });
            done();
          };

          users.once( 'op', callback );

          users
            .findAndModify( { _id: user._id.toString() }, { $pull: { a: 'woot' } } )
            .catch( ( e ) => {
              users.removeListener( 'op', callback );
              done( e );
            } );  
        } )
        .catch( ( e ) => done( e ) );
    });

    it('should work with findAndModify (shorthand)', function(done){
      users
        .insert( {} )
        .then( ( user ) => {
          const callback = function(id, query, op){
            expect(id).to.be(user._id.toString());
            expect(query).to.eql({});
            expect(op).to.eql({ $pull: { a: 'woot' } });
            done();
          };

          users.once( 'op', callback );

          users
            .findAndModify({ query: user._id.toString(), update: { $pull: { a: 'woot' } } })
            .catch( ( e ) => {
              users.removeListener( 'op', callback );
              done( e );
            } );
        })
        .catch( ( e ) => done( e ) );
    });

    it('should work with findAndModify (shorthand#2)', function(done){
      users
        .insert( {} )
        .then( ( user ) => {
          const callback = function(id, query, op){
            expect(id).to.be(user._id.toString());
            expect(query).to.eql({});
            expect(op).to.eql({ $pull: { a: 'woot' } });
            done();
          };

          users.once( 'op', callback );

          users
            .findAndModify({ _id: user._id.toString() }, { $pull: { a: 'woot' } })
            .catch( ( e ) => {
              users.removeListener( 'op', callback );
              done( e );
            } );
        })
        .catch( ( e ) => done( e ) );
    });

    it('should work with findAndModify (shorthand#3)', function(done){
      users.insert({}, function(err, user){
        expect(err).to.be(null);
        users.findAndModify(user._id.toString(), { $pull: { a: 'woot' } }, function(err){
          expect(err).to.be(null);
        });
        users.once('op', function(id, query, op){
          expect(id).to.be(user._id.toString());
          expect(query).to.eql({});
          expect(op).to.eql({ $pull: { a: 'woot' } });
          done();
        });
      });
    });

    it('should work with non-multi update', function(done){
      users.insert({ a: 'haha' }, function(err, user){
        if (err) return done(err);
        users.update({ a: 'haha' }, { $set: { a: 'b' } }, function(err){
          expect(err).to.be(null);
        });
        users.once('op', function(id, query, op){
          expect(id).to.be(user._id.toString());
          expect(query).to.eql({});
          expect(op).to.eql({ $set: { a: 'b' } });
          users.findOne({ a: 'b' }, function(err, u){
            expect(err).to.be(null);
            expect(u._id.toString()).to.be(id);
            done();
          });
        });
      });
    });

    it('should work with non-multi update that noops', function(done){
      users.update({ a: Date.now() }, { $set: { a: 'b' } }, function(err){
        expect(err).to.be(null);
        done();
      });
    });

    it('should not work multi update', function(done){
      users.insert({ test: 'test' }, function(err, user){
        users.once('op', function onop(){
          throw new Error('Nope');
        });

        users.update({ test: 'test' }, { $set: { test: 'a' } }, { multi: true }, function(err){
          expect(err).to.be(null);
          users.findOne({ test: 'a' }, function(err, us){
            expect(err).to.be(null);
            expect(us).to.be.an('object');
            users.removeAllListeners('op');
            done();
          });
        });
      });
    });
  });

  describe('redis', function(){
    var db = mydb('localhost:31003/mydb-driver-test', { redisPort: 31001 });
    var users = db.get('users');

    var sub = redis.createClient( { url: 'redis://127.0.0.1:31001' } );
    
    it('should get ops published', function( done ) {
      sub.connect()
      .then( () => {
        users
          .insert({})
          .then( ( user ) => {
            const callback = function(id, query, op){
              expect(id).to.equal(user._id.toString());
              expect(query).to.eql({});
              expect(op).to.eql({ $set: {} });
            };

            users.on( 'op', callback );

            sub.subscribe( user._id.toString(), function(msg, channel){
              expect(channel).to.be(user._id.toString());
              var obj = JSON.parse(msg);
              expect(obj[0]).to.eql({});
              expect(obj[1]).to.eql({ $set: {} });
              done();
            } );

            users
              .update( { _id: user._id }, { $set: {} } )
              .catch( ( e ) => {
                users.removeListener( 'op', callback );
              } );
          } )
          .catch( ( e ) => done( e ) );
      })
      .catch( ( e ) => done( e ) );
    });

  });

});
