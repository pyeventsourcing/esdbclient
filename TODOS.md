## For version 1.0

* Cluster support & connectivity (different connections strings + node pref);
  * Support the esdb:// and esdb+discover:// URLs
    * which parameters are there to support?
    * which parameters should we support?
* Delete stream;
* Read / modify stream metadata;
  * Put two dollars in front of it....
* Doc & sample.

* Check "subscription confirmed" of read response?
* Support filtering on stream name (currently just event type)?

* OAuth (requires commercial license)?
* Connect to cloud service? TailScale VPN... a bit complicated

* Tombstone stream?

Questions:

* What is the 'structured' alternative for UUIDOption?

* Can't filter persistent subscription to a stream when using StreamOptions, because it doesn't have filter_option field (only AllOptions does)?
  * TBF, where the option does exist, it's not a legal combination... just can't filter reading or subscribing to a stream?
* Is there a difference between a persistent subscription that uses StreamOptions, and one that uses AllOptions with stream identifier expression that matches a single stream identifier?
  * can use filter options with AllOptions? why not also with StreamOptions, like with catchup subscription?
* Which content-type variations are possible? which are common? which is most usual?
* Is it really the case that commit_position is not set on recorded events in v21.10? how then are downstream processors supposed to keep track of position?
  * Ans: this seems to be only when reading events from a stream in v21.20, reading all events does return commit position...
* What other types are used for "system events" other than '$.+' and 'PersistentSubscription\\d+'? Let's say I want only the events I have appended, what exclude filter do I use?
* Why isn't the "type" of 'PersistentSubscriptionX" events a past participle?

* Why does the gRPC error say 'ALREADY_EXISTS' when appending batch to stream that does not exist with "stream_position" set, when appending single event to stream that does not exist with "revision" gives response with "current_no_stream?
  * that is, batch append response doesn't discriminate between the two cases, although append response does - am I missing something?
* In persistent.proto, is ReadResp.position ever "no_position"? or is there always a commit position now?
