haskell-mongodb-queue
=====================

inspiration from MongoMQ

[Initial introductory blog post](http://blog.docmunch.com/blog/2013/announce-haskell-mongodb-queue-library)

See the [haddocks](http://hackage.haskell.org/package/mongodb-queue).

A simple messaging queue using MongoDB. This trades having a good queue for ease of deployment. This is designed to be much worse at scale than real queueing infrastructure. However, it is very simple to start using if you are already running MongoDB. You could probably fork this code to make it work with a different database that you are already using.
.
There are 2 options for receiving a message: polling or tailable cursors. Polling is obviously inefficient, but it works against an index on a capped collection, so it should still be fairly efficient, and as fast as the polling interval you set. Tailable cursors respond very quickly and don't re-query the database. However, there is an outstanding bug that they use up CPU on the database when the system is idle, particularly as more tailable cursors are added. The idle CPU usage will become worse as you scale out to multiple worker processes.
