
create table if not exists hub_instrument (
    PK              text primary key,
    date_load       integer not null,
    record_source   text not null,
    isin            text not null,
    unique(isin)
);

create table if not exists sat_XMUN_tradable_instrument_info (
    PK              text primary key references hub_instrument(PK),
    date_load       integer not null,
    record_source   text not null,
    wkn             text,
    name            text,
    title           text,
    symbol          text,
    type            text,
    unique(PK)
);

create table if not exists sat_instrument_symbol_1d (
    PK_hub        text not null references hub_instrument(PK),
    date_load     integer not null,
    record_source text not null,
    date          integer,
    open          real,
    high          real,
    low           real,
    close         real,
    volume        integer,
    dividend      real,
    stock_split   real,
    unique(PK_hub, record_source, date)
);

create table if not exists sat_instrument_symbol_1h (
    PK_hub        text not null references hub_instrument(PK),
    date_load     integer not null,
    record_source text not null,
    date          integer,
    open          real,
    high          real,
    low           real,
    close         real,
    volume        integer,
    unique(PK_hub, record_source, date)
);

create table if not exists sat_instrument_symbol_1m (
    PK_hub        text not null references hub_instrument(PK),
    date_load     integer not null,
    record_source text not null,
    date          integer,
    open          real,
    high          real,
    low           real,
    close         real,
    volume        integer,
    unique(PK_hub, record_source, date)
);
