CREATE KEYSPACE events
WITH REPLICATION = {
 'class': 'SimpleStrategy',
 'replication_factor': 2
 };

use events;

CREATE TABLE events_mentions (
 globalEventsID int,
 eventDate date,
 mentionDateTime timestamp,
 mentionSourceName text,
 confidence int,
 mentionDocTone float,
 firm text,
 sector text,
 country text,
 PRIMARY KEY ((sector, firm), mentionDateTime)
);

CREATE OR REPLACE FUNCTION toneScore (tone FLOAT, confidence INT)
RETURNS NULL ON NULL INPUT
RETURNS double LANGUAGE java AS
'return Double.valueOf(tone.doubleValue() * confidence.intValue())';


COPY events_mentions_country
(sector, firm, mentiondatetime, confidence, country, eventdate, globaleventsid, mentiondoctone, mentionsourcename)
FROM 'all_events_mentions_by_sources.csv' WITH HEADER=true;