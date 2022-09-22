
-- creates databases with preconfigured tables and user
-- database name: INVDB
-- ETL user name: RUSER
EXEC sp_configure 'show advanced', 1
GO
RECONFIGURE
GO
EXEC sp_configure 'contained database authentication', 1
GO
RECONFIGURE
GO

USE [master]
GO
CREATE DATABASE [FUND] ON 
( FILENAME = N'/var/opt/mssql/data/invdb.mdf' ),
( FILENAME = N'/var/opt/mssql/data/invdb_log.ldf' )
 FOR ATTACH
GO
