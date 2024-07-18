CREATE DATABASE "YouTubeViewPrediction"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

CREATE SCHEMA IF NOT EXISTS public
    AUTHORIZATION pg_database_owner;

CREATE TABLE IF NOT EXISTS public.video_fact
(
    collected_at timestamp without time zone NOT NULL,
    video_id character varying(11) COLLATE pg_catalog."default" NOT NULL,
    num_views integer NOT NULL,
    num_likes integer NOT NULL,
    num_comments integer NOT NULL,
    CONSTRAINT pk_1 PRIMARY KEY (collected_at, video_id)
);

CREATE TABLE IF NOT EXISTS public.video_dim
(
    video_id character varying(11) COLLATE pg_catalog."default" NOT NULL,
    channel_id character varying(24) COLLATE pg_catalog."default" NOT NULL,
    video_title character varying(100) COLLATE pg_catalog."default" NOT NULL,
    video_description character varying(5000) COLLATE pg_catalog."default" NOT NULL,
    num_tags integer NOT NULL,
    duration_seconds integer NOT NULL,
    licensed_content boolean NOT NULL,
    made_for_kids boolean NOT NULL,
    published_at timestamp without time zone NOT NULL,
    category_id integer NOT NULL,
    CONSTRAINT pk_2 PRIMARY KEY (video_id)
);

CREATE TABLE IF NOT EXISTS public.channel_dim
(
    channel_id character varying(24) COLLATE pg_catalog."default" NOT NULL,
    channel_name character varying COLLATE pg_catalog."default" NOT NULL,
    created_datetime timestamp without time zone NOT NULL,
    CONSTRAINT pk_3 PRIMARY KEY (channel_id)
);

CREATE TABLE IF NOT EXISTS public.channel_fact
(
    collected_at timestamp without time zone NOT NULL,
    channel_id character varying(24) COLLATE pg_catalog."default" NOT NULL,
    channel_total_views bigint NOT NULL,
    num_subscribers integer NOT NULL,
    num_videos integer NOT NULL,
    CONSTRAINT pk_4 PRIMARY KEY (collected_at, channel_id)
);

CREATE TABLE IF NOT EXISTS public.categories_dim
(
    category_id integer NOT NULL,
    category_name character varying(21) COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT pk_5 PRIMARY KEY (category_id)
);