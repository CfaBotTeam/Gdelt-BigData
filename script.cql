CREATE KEYSPACE events
WITH REPLICATION = {
 'class': 'SimpleStrategy',
 'replication_factor': 2
 };

use events;

CREATE TABLE events_mentions (
 globalEventsID int,
 eventDateTime date,
 mentionDateTime date,
 mentionSourceName text,
 confidence int,
 mentionDocTone float,
 firm text,
 sector text,
 country text,
 PRIMARY KEY ((sector, firm), mentionDateTime)
);
