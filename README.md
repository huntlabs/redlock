[![Build Status](https://travis-ci.org/huntlabs/redlock.svg?branch=master)](https://travis-ci.org/huntlabs/redlock)
# redlock

Distributed locks with Redis . 
Implement in dlang by https://redis.io/topics/distlock.

# example
````d	

import redlock.RedLock;

auto lock = new RedLock("127.0.0.1:6379");
// save locked info
LockedObject obj;
lock.Lock("key" , obj);
//do some thing
lock.Unlock(obj);


````	
	
