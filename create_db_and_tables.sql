CREATE DATABASE "YouTubeViewPrediction"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LOCALE_PROVIDER = 'libc'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

CREATE TABLE IF NOT EXISTS video_fact
(
    collected_at timestamp NOT NULL,
    video_id     varchar(11) NOT NULL,
    num_views    int NOT NULL,
    num_likes    int NOT NULL,
    num_comments int NOT NULL,
    CONSTRAINT PK_1 PRIMARY KEY ( collected_at, video_id )
);

CREATE TABLE IF NOT EXISTS video_dim
(
    video_id      varchar(11) NOT NULL,
    channel_id    varchar(24) NOT NULL,
    title         varchar(100) NOT NULL,
    description   varchar(5000) NOT NULL,
    num_tags      int NOT NULL,
    duration_sec  int NOT NULL,
    licensed      boolean NOT NULL,
    made_for_kids boolean NOT NULL,
    published_at  timestamp NOT NULL,
    category_id   int NOT NULL,
    CONSTRAINT PK_2 PRIMARY KEY ( video_id )
);

CREATE TABLE IF NOT EXISTS channel_dim
(
    channel_id varchar(24) NOT NULL,
    name       varchar(36) NOT NULL,
    created_at timestamp NOT NULL,
    CONSTRAINT PK_1 PRIMARY KEY ( channel_id )
);

CREATE TABLE IF NOT EXISTS channel_fact
(
    datetime        timestamp NOT NULL,
    channel_id      varchar(24) NOT NULL,
    total_views     int NOT NULL,
    num_subscribers int NOT NULL,
    num_videos      int NOT NULL,
    CONSTRAINT PK_1 PRIMARY KEY ( datetime, channel_id )
);

CREATE TABLE IF NOT EXISTS categories_dim
(
    category_id int NOT NULL,
    name        varchar(21) NOT NULL,
    CONSTRAINT PK_1 PRIMARY KEY ( category_id )
);