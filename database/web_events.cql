CREATE KEYSPACE web_events_analysis WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE web_events_analysis.raw_events_data (
year int,
month int,
day int,
hour int,
minutes int,
event_id timeuuid,
message_id bigint,
timestamp timestamp,
visit_origin text,
location text,
department text,
product_id int,
quantity int,
action text,
transaction_id bigint,
payment_type text,
shipment_type text,
PRIMARY KEY ((year, month, day, hour), event_id)
);

CREATE TABLE web_events_analysis.events_per_location_per_hour_batch (
year int,
month int,
day int,
hour int,
location text,
count bigint,
PRIMARY KEY (year, month, day, hour, location)
);

CREATE TABLE web_events_analysis.events_per_location_per_day_batch (
year int,
month int,
day int,
location text,
count bigint,
PRIMARY KEY (year, month, day, location)
);

CREATE TABLE web_events_analysis.events_per_location_per_month_batch (
year int,
month int,
location text,
count bigint,
PRIMARY KEY (year, month, location)
);

CREATE TABLE web_events_analysis.events_per_location_per_year_batch (
year int,
location text,
count bigint,
PRIMARY KEY (year, location)
);