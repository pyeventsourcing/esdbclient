## For version 1.0

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

Questions:

* Can't filter persistent subscription to a stream when using StreamOptions, because it doesn't have filter_option field (only AllOptions does)
* What's the difference between a persistent subscription that uses StreamOptions, and one that uses AllOptions with stream identifier expression that matches a single stream identifier? can filter with AllOptions?
* Does a persistent subscription never return "system" events?
