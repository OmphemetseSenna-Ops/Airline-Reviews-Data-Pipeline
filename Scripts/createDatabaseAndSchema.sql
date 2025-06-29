-- Create the database
USE master;
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'BritishAirwaysDW')
BEGIN
    ALTER DATABASE BritishAirwaysDW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE BritishAirwaysDW;
END
GO

CREATE DATABASE BritishAirwaysDW;
GO

USE BritishAirwaysDW;
GO



-- Create the required schemas
CREATE SCHEMA dim;
GO

CREATE SCHEMA fact;
GO

CREATE SCHEMA audit;
GO