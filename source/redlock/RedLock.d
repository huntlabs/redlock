﻿module redlock.RedLock;

import std.uuid;
import std.string;
import std.random;
import std.conv;
import std.stdio;

import core.time;
import core.thread;
import core.stdc.stdlib;

import redis;



struct LockedObject
{
	string  key;
	string  uniqueId;
	ulong 	validTime;
}


class RedLock
{

	this(const string redis_servers , bool initConnected = true){

		auto hostports = redis_servers.split(";");
		foreach( hp ; hostports)
		{
			if(hp == string.init)
				continue;

			string[] hpc = hp.split(":");

			try{
				_redisClients ~= new Redis(hpc[0] , to!ushort(hpc[1]));
			}
			catch(Throwable e)
			{
				if(initConnected)
					 throw e;
				else
					_hostports[hp] = true;
			}
		
		
		}

		_quornum = _redisClients.length / 2 + 1;
	
		_delaytime = 10;
		_clockFactor = 0.01;
	}


	bool Lock(const string key , ref LockedObject lock ,
		uint timeout = uint.max , uint ttl = 60000  )
	{
		string val = to!string(randomUUID());
		auto end_tick = nsecsToTicks(cast(long)timeout * 1000 * 1000) + MonoTime.currTime.ticks();
		synchronized(this)
		{
			lock.key = key;
			lock.uniqueId = val;

			do{
				ulong n = 0;
				auto t1 = MonoTime.currTime.ticks();
				foreach(c ; _redisClients)
				{
					if(LockInstance(c , key, val , ttl)) ++n;
				}

				auto t2 =  MonoTime.currTime.ticks();
				auto clockdrat = cast(ulong)(_clockFactor * ttl) + 2;
				ulong validtime = ttl - ticksToNSecs(t2 - t1)/1000 - clockdrat;
				if(validtime > 0 && n >= _quornum)
				{
					lock.validTime = validtime;
					return true;
				}else{
					Unlock(lock);
				}
				ulong delay = rand() % _delaytime + _delaytime / 2;
				Thread.sleep(dur!"msecs"(delay));
			}while(MonoTime.currTime.ticks() < end_tick);

			return false;
		}

	}

	void Unlock(const ref LockedObject lock)
	{
		synchronized(this)
		{
			foreach(c ; _redisClients)
			{
				UnlockInstance(c , lock.key  , lock.uniqueId);
			}

			if(_hostports !is null)
			{
				foreach(k  ; _hostports.keys)
				{
					auto hpc = k.split(":");
					try{
						_redisClients ~= new Redis(hpc[0] , to!ushort(hpc[1]));
						_hostports.remove(k);
					}
					catch(Throwable e)
					{

					}
				}
			}
		}

	}

private:

	bool LockInstance(Redis redis , const string key , const string value ,  uint ttl)
	{
		try{
			return redis.send!bool("set" , _prefix ~ key , value , "px" , ttl , "nx");
		}
		catch(Throwable e)
		{
			return false;
		}
	}

	void UnlockInstance(Redis redis , const string key , const string value)
	{
		try{
			redis.eval(`if redis.call('get', KEYS[1]) == ARGV[1] 
							then return redis.call('del', KEYS[1])
						else 
							return 0 
						end`,[_prefix ~ key] , [value]);
		}
		catch(Throwable e)
		{

		}

	}

	Redis[]						_redisClients;

	bool[string]				_hostports;

	immutable ulong				_quornum;
	immutable ulong 			_delaytime = 10;
	immutable float 			_clockFactor = 0.01;


	static immutable string		_prefix	= "Dlang_RedLock_";
}

unittest{


	import core.thread;
	import std.stdio;
	import std.conv;
	class Test:Thread
	{
		string 	_name;  
		int 	_second;
		bool  	_flag;
		RedLock _lock;
		this(string name , int second)
		{
			super(&run);
			_name = name;
			_second = second;
			_lock = new RedLock("127.0.0.1:6379");
			_flag = true;
		}

		void stop()
		{
			_flag = false;
		}

		void run()
		{
			while(_flag)
			{
				LockedObject obj;
				if(!_lock.Lock("test1" ,obj , 1000))
				{
					writeln(" timeout failed ", _name);
					continue;
				}

				writeln(_name , " locked");
				Thread.sleep(dur!"msecs"(500));
				writeln(_name , " un locked");
				_lock.Unlock(obj);
				Thread.sleep(dur!"seconds"(1));
			}

		}
	}


	Test[] list;
	for(uint i = 0 ; i < 100 ; i ++)
	{
		auto test = new Test(to!string(i) , 1);
		test.start();
		list ~= test;
	}

	Thread.sleep(dur!"seconds"(30));

	foreach(t ; list)
	{
		t.stop();
	}

	foreach( t; list)
		t.join();

}