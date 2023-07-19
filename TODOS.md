## For version 1.0

* Something about causation and correlation IDs?

* Commit position vs prepare position?
  * As I understand it, these values only differ when using TCP/IP client transaction?
    * So probably only used by a small number of people, if any?

* More persistent subscription options
  * https://developers.eventstore.com/clients/grpc/persistent-subscriptions.html#persistent-subscription-settings

* Doc
  * Exception classes: describe which methods might raise which exceptions and why?
  * Is the README file sufficient for v1.0? or do I need to use Sphinx?
  * ClusterMember data class attributes (there are quite a lot of them...)

* Sample
  * Is the eventsourcing-eventstoredb package sufficient for v1.0?
  * Should I port Joao's banking app?

* OAuth (requires commercial license)
  * What if anything would be needed in the client to support OAuth?

* Change GitHub workflow to also test with 22.10.0?
  * 22.10.0 seems a little buggy (see tests)?
    * Actually, occasionally getting "Exception was thrown by handler" from 21.10.9...

* I noticed the issue raised about "consumer too slow" - does the server close subscriptions?
  * what actually does the server do in this case? end the call? send a ReadResp?
    * "Bear in mind that a subscription can also drop because it is slow. The server tried to push all the live events to the subscription when it is in the live processing mode. If the subscription gets the reading buffer overflow and won't be able to acknowledge the buffer, it will break."
      * from https://developers.eventstore.com/clients/grpc/subscriptions.html#dropped-subscriptions

* I noticed that when subscribing to all with a catch-up subscription, a checkpoint is
    received when there are no more recorded events which has a commit position that
    at which there is no recorded event. This commit position is allocated to the next
    new event which is recorded. Catch-up subscriptions started with a specific commit
    position do not return the event at that commit position. So if an event processing
    component records progress using the checkpoint commit positions, and restarts
    catch-up subscription using the recorded commit positions, and happens to record
    the commit position of a checkpoint received because there are no more recorded
    events, and then happens to crash and be restarted, and then a new event is appended,
    it will never receive that new event.... And also if you screen out responses by
    looking for repeat commit positions, then you will see the checkpoint first, and
    then ignore the subsequent event that has the same commit position.
  * See: test_checkpoint_commit_position()

-----
Issues:

* Server errors on GitHub Actions:
  * In README.md assert acked_events[event9.id] == 0, acked_events[event9.id]  # sometimes this isn't zero?
    * this is because the client is "too slow" so that the server retries to send the event
  * In README.md, sometimes we don't receive event9 from persistent subscription, even though we received it from catchup subscription
  * In README.md, sometimes catchup subscription gives unexpected results.
  * In cluster info, sometimes get status UNKNOWN
    * this can happen when leader election timesout (on GHA prob due to virtualisation)
  * In various methods, occasionally get "Exception was thrown by handler" - do I need to get the logs?
    * In test, test_reconnects_to_new_leader_on_append_events() especially often for some reason.
  * In test_reconnects_to_new_leader_on_set_stream_metadata, occasionally get FollowerNotFound, despite previous and subsequent tests passing okay, as if gossip is a bit flaky.


* What is the "requires-leader" header actually for, since the server can decide if
  methods require leader? should this instead be something the client sets according to
  its node preference?
  * Seems that "requires-leader" isn't implemented on server for list subscriptions?
    * getting from follower: status code: UNAVAILABLE, details: "Server Is Not Ready", rather than "Leader info available"

* Update persistent subscription
  * very similar to Create (just without filter options)
    * Qn: Why is named_consumer_strategy deprecated in CreateReq but not UpdateReq?

* Connection field 'TlsVerifyCert' - what to verify? and how?
  * design intention: if "false" configure client to avoid checking server certificate
    * not sure that we can do this with the Python library, although there is a callback...
      * there is a discussion about this on GH, with a PR that wasn't merged
        * https://github.com/grpc/grpc/issues/15461



-----

Notes 21 February 2023:
* The append operation is actually atomic for the whole request stream.
* The BatchAppendResp is given:
  * when there's an error
  * when transaction / atomic commit is done
  * when call completed (Joao is not sure why this in there...)
* Consider opening gRPC batch append call immediately when the client is constructed
  and then have it pull from a queue of batch append request futures, and then
  send batch append requests and handle the batch append responses by setting
  the result on the future - the client method would then get a future and wait
  for the result.
* In persistent.proto, is ReadResp.position ever "no_position"? or is there always a commit position now (in 22.10)?
  * still might not have when reading events from a stream if those events have been committed through a TCP/IP interface transaction
    * also applies to regular reading...
  * note: transaction isolation is "read-commited", and such events do have commit position when read from "all streams"
* Need to return prepare_position (but which commit_position. response.event.commit_position or response.event.event.commit_position)
* Why isn't the "type" of 'PersistentSubscriptionX" events a past participle?
  * Ans: it just isn't :)
* What other types are used for "system events" other than '$.+' and 'PersistentSubscription\\d+'? Let's say I want only the events I have appended, what exclude filter do I use?
  * None, just those. They are all just "PersistentSubscription1" at the moment...
* Which content-type variations are possible? which are common? which is most usual?
  * Ans: there are just these two supported ATM, future plans for other things e.g. scheme registry
  * Perhaps actually subclass JSONNewEvent and BytesNewEvent so that it's more explicit
* Is there a difference between a persistent subscription that uses StreamOptions, and one that uses AllOptions with stream identifier expression that matches a single stream identifier?
  * Ans: the stream identifier in AllOptions is a regular expression, so can match a selection of streams
* Can use filter options with AllOptions? why not also with StreamOptions, like with catchup subscription?
  * Ans: you can't use filter with StreamOptions :) the proto is different, because development
* What is the 'structured' alternative for UUIDOption?
  * Ans: it's just for carrying UUID as two smaller integers, for compatibility with languages that can't deal with larger integers
* What is 'window'?
  * Ans: the number of events excluded by the filter before sending matching events that are undersized for the "page / response chunk"
    * can we set the size of the page? no
* What is 'checkpointIntervalMultiplier'?
  * Ans: when reading events with large gap, have to wait for DB to go through all the intermediate records, and this avoids having
    to repeat all that if we crash and have to resume from the position point before the gap of the last event that was processed.
  * This is the number of window sized gaps before sending a "checkpoint"
* Support filtering on stream name (currently just event type)?
  * no (?)
* If you delete a stream, can write to stream, but need correct expected version
  (can't use position 0 again) and if OCC is disabled, then the stream positions
  will increment from the last (deleted) one
* If you tombstone a stream, then it's gone forever, and you can never get it back
  * Qn: is there a way of getting a deleted stream back?
    * Ans:
  * Qn: what happens when try to append to a tombstoned stream? not sure... try it :-)
    * Ans: response is a "StreamDeleted" error
* Connect to cloud service?
  * Need TailScale VPN - a bit complicated, but doesn't have any consequence for client
* Check "SubscriptionConfirmation confirmation" of read response?
  * in catch-up subscription, server telling client that subscription is working (will get data soon)
  * saves not knowing whether subscription request was successfully received or not
* Read / modify stream metadata;
  * Metadata is a stream, stream name is two dollars in front of the stream name
    * does this have consequences in the database?
      * yes, has ACLs, "max-age", "max-count", "truncate before"
  * To modify, read last, parse as JSON, make changes, serialise as JSON, then append new event
  * See https://developers.eventstore.com/server/v22.10/streams.html#metadata-and-reserved-names
* Cluster support & connectivity (different connections strings + node pref);
  * Support the esdb:// and esdb+discover:// URLs
    * which parameters are there to support?
    * See: https://github.com/EventStore/EventStore-Client-Dotnet/blob/master/src/EventStore.Client/EventStoreClientSettings.ConnectionString.cs#L28

        * Tls [true|false] (default: true, controls whether to use a secure channel? has to match with the database)
        * ConnectionName (required, default is random UUID, send this as HTTP header: "connection-name")
        * MaxDiscoverAttempts (default: 10, num attempts to connect to gossip before giving up)
        * DiscoveryInterval (default: 100ms, how long to wait between gossip retries)
        * GossipTimeout (default: 5s, how long to wait for a response to a request to gossip API)
        * NodePreference (default: leader, see below)
        * TlsVerifyCert [true|false] (default: true, controls what?)
        * DefaultDeadline (the default timeout value for calls) - although, reads should have no timeout value (i.e. timeout=None)
        * ThrowOnAppendFailure [true|false] (no default, a .Net thing, whether to either raise exception or return error response)
        * KeepAliveInterval (gRPC channel option)
        * KeepAliveTimeout (gRPC channel option)

        * Rule 1: make sure all keys and values are case-insensitive
        * Rule 2: schemas
            * esdb://host1:port,host2:port,host3:port/  multiple sockets
            * esdb+discover://host1/ - this means get the list of hosts is listed in DNS records under this name - not sure where the port number goes
        * Rule 3: node preference controls whether requests are directed to another node
          * NodePreference: leader, follower, random, readonlyreplica
          * need to make a call to the gossip API, either timeout or give response, hence
            discover cluster topology, then based on node preference need to connect to
            appropriate node - also need to send HTTP header, depending on call ("requires-leader: [true|false]")
            * Use this for testing with a cluster: https://github.com/EventStore/EventStore/blob/master/docker-compose.yml
              * modify "build: ./" to use the latest Docker image
              * ask Joao about how to detect which node is the leader (an HTTP GET request?)
            * close and reopen channel if we need to switch to a different server if topology changes so that leader is now a follower
              * similarly, if node preference is follower or readonlyreplica, in future switch (but don't for now - it's "best effort")
          * https://github.com/EventStore/EventStore-Client-Dotnet/blob/master/src/EventStore.Client.Common/EventStoreCallOptions.cs#L26
        * Rule 4: user info, separated by '@'
        * Rule 5: Need to set gRPC channel option: "grpc.max_receive_message_length": 17 * 1024 * 1024
          * because otherwise it might be too small
          * also should never write events this big - maybe enforce this?



----

Notes from 14 April 2023:
* mindsdb.com
