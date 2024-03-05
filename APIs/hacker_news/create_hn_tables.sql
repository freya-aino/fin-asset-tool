
create table if not exists blob (
    hn_id                   integer     primary key not null,
    dt_load                 integer,
    is_deleted              integer     check (is_deleted == 0 or is_deleted == 1),
    is_dead                 integer     check (is_dead == 0 or is_dead == 1),
    type                    type,
    author                  text,
    dt_source_created       integer,
    title                   text,
    text                    text,
    parent_hn_id            integer,
    poll_hn_id              integer,
    score                   integer,
    poll_opt_hn_id_list     text,
    children_hn_id_list     text,
    descendants_hn_id_list  text,
    unique(hn_id));

create table if not exists story (
    story_id            integer     primary key autoincrement,
    hn_id               integer,
    author              text,
    is_deleted          integer     check (is_deleted == 0 or is_deleted == 1),
    is_dead             integer     check (is_dead == 0 or is_dead == 1),
    dt_created          integer,
    title               text,
    score               integer,
    number_descendants  integer,
    text                text,
    children            text,
    url                 text,
    unique(hn_id));

create table if not exists comment (
    comment_id          integer     primary key autoincrement,
    hn_id               integer,
    author              text,
    is_deleted          integer     check (is_deleted == 0 or is_deleted == 1),
    is_dead             integer     check (is_dead == 0 or is_dead == 1),
    dt_created          integer,
    text                text,
    parent_comment_id   integer,
    parent_story_id     integer,
    children            text,
    unique(hn_id));

create table if not exists poll (
    poll_id             integer     primary key autoincrement,
    hn_id               integer,
    author              text,
    is_deleted          integer     check (is_deleted == 0 or is_deleted == 1),
    is_dead             integer     check (is_dead == 0 or is_dead == 1),
    dt_created          integer,
    title               text,
    text                text,
    poll_opts           text,
    number_descendants  integer,
    score               integer,
    children            text,
    unique(hn_id));

create table if not exists poll_opt (
    poll_opt_id         integer     primary key autoincrement,
    hn_id               integer,
    author              text,
    is_deleted          integer     check (is_deleted == 0 or is_deleted == 1),
    is_dead             integer     check (is_dead == 0 or is_dead == 1),
    dt_created          integer,
    poll_id             integer     not_null,
    score               integer,
    text                text,
    related_pollopts    text,
    unique(hn_id));

commit;
