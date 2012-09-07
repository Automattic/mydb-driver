
# mydb-driver

mydb monk driver (primarily for development)

## how to use

```js
var db = require('mydb-driver')('localhost/test');
var users = db.get('users');

users.update(id, { $set: { a: 'b' } })
users.on('op', function(id){});
// ops are published to redis for each object id in addition to the `op` event
```

### Custom redis host/port

```js
var db = require('mydb-driver')('localhost/test', {
  redisHost: '',
  redisPort: 6679
});
```

### Custom redis client

```js
var db = require('mydb-driver')('localhost/test', { redis: myRedis });
```
