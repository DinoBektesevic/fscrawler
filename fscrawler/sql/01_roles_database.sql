-- set up admin, manager and user roles
CREATE ROLE crawler_admin_role;
CREATE ROLE crawler_manager_role;
CREATE ROLE crawler_user_role NOLOGIN;

-- Then inherit from them so that we can later bifurcate the
-- database ownership and permissions from USER ROLES and ROLES.
CREATE USER crawler_admin   WITH PASSWORD 'pass' IN ROLE crawler_admin_role;
CREATE USER crawler_manager WITH PASSWORD 'word' IN ROLE crawler_manager_role;
CREATE USER crawler_user in ROLE crawler_user_role;

-- Create the database
CREATE DATABASE fscrawler OWNER crawler_admin_role;

-- allow the user roles to login
GRANT CONNECT ON DATABASE fscrawler TO crawler_manager;
GRANT CONNECT ON DATABASE fscrawler TO crawler_user;

-- set the default search_path for all connections to the DB
-- I tried making this explicit in the codebase, so technically
-- this isn't required, but then one needs to reconnect as postgres
-- to set the deufalt paths per role, unless they feel like the
-- effort to try and manage explicit schema prefixes worth it.
-- I don't, so leave this here for now.
ALTER DATABASE fscrawler SET search_path TO crawler;
