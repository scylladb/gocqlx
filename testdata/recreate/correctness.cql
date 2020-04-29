
CREATE KEYSPACE gocqlx_correctness WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

CREATE TYPE gocqlx_correctness.phone (
    country_code int,
    number text
);

CREATE TYPE gocqlx_correctness.address (
    street text,
    city text,
    zip text,
    phones map<text, frozen<phone>>
);

CREATE TABLE gocqlx_correctness.restaurants (
    location text,
    name text,
    long_name text,
    address frozen<address>,
    PRIMARY KEY(location, name)
);

CREATE INDEX ON gocqlx_correctness.restaurants(name);
CREATE INDEX ON gocqlx_correctness.restaurants((location), name);


CREATE MATERIALIZED VIEW gocqlx_correctness.restaurants_by_long_name AS
    SELECT * FROM restaurants
    WHERE location IS NOT NULL AND long_name IS NOT NULL AND name IS NOT NULL
    PRIMARY KEY(location, name, long_name);