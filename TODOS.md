## For version 1.0

* Change append_events() to use batch, and add an append_event() method.

* Cluster support & connectivity (different connections strings + node pref, TLS
support);
  * Use the esdb:// and esdb+discover:// URLs
    *
  * Three options for authentication:
    * username/password (default: "admin/changeit")
    * SSL certificates
    * OAuth (requires commercial license)
* Read / append / delete stream;
* Read / modify stream metadata;
  * Put two dollars in front of it....
* Catch-up subscription (stream + $all);
* Persistent subscription (stream + $all);
* Doc & sample.

* JSON/bytes new event (https://github.com/pyeventsourcing/esdbclient/issues/5)

* Idempotent writes...
  * Make append request with the same UUID as the ProposedMessage.id

* Connect to cloud service... TailScale VPN... a bit complicated

* Tombstone stream?

Questions:

* Can't filter persistent subscription to a stream when using StreamOptions, because it doesn't have filter_option field (only AllOptions does)
* Is there a difference between a persistent subscription that uses StreamOptions, and one that uses AllOptions with stream identifier expression that matches a single stream identifier?
  * can use filter options with AllOptions? why not also with StreamOptions, like with catchup subscription?
* Which content-type variations are possible? which are common? which is most usual?
* Is it really the case that commit_position is not set on recorded events in v21.10? how then are downstream processors supposed to keep track of position?
  * Ans: this seems to be only when reading events from a stream in v21.20, reading all events does return commit position...
* What other types are used for "system events" other than '$.+' and 'PersistentSubscription\\d+'? Let's say I want only the events I have appended, what exclude filter do I use?
* Why isn't the "type" of 'PersistentSubscriptionX" events a past participle?

* Why does the gRPC error say 'ALREADY_EXISTS' when appending batch to stream that does not exist with "stream_position" set, when appending single event to stream that does not exist with "revision" gives response with "current_no_stream?
  * that is, batch append response doesn't discriminate between the two cases, although append response does - am I missing something?
