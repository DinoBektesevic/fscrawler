CREATE SCHEMA IF NOT EXISTS crawler AUTHORIZATION crawler_admin_role;

GRANT USAGE ON SCHEMA crawler TO crawler_manager_role;
GRANT USAGE ON SCHEMA crawler TO crawler_user_role;

-- See default search_path comment in 01 script.
-- if that is ever removed, you need to uncomment this
-- or prefix all interactive sessions with schema name
-- which is a pain.
-- /connect postgres postgres
-- ALTER ROLE crawler_admin SET search_path TO crawler;
-- ALTER ROLE crawler_manager SET search_path TO crawler;
-- ALTER ROLE crawler_user SET search_path TO crawler;
-- /connect fscrawler_admin

-- Manager role privileges (everything except drop)
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA crawler
    TO crawler_manager_role;

-- User is readonly
GRANT SELECT
    ON ALL TABLES IN SCHEMA crawler
    TO crawler_user_role;

-- Auto-apply priviledges on all tables in schema, including those created in future.
ALTER DEFAULT PRIVILEGES IN SCHEMA crawler
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
    ON TABLES TO crawler_manager_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA crawler
    GRANT SELECT
    ON TABLES TO crawler_user_role;

-- sequences are needed for BIGSERIAL inserts
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA crawler TO crawler_manager_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA crawler
    GRANT USAGE, SELECT ON SEQUENCES TO crawler_manager_role;
