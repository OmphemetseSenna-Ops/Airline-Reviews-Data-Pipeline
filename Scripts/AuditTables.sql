USE BritishAirwaysDW;
GO

-- Create ETL Batch table
CREATE TABLE audit.ETLBatch (
    BatchID INT IDENTITY(1,1) PRIMARY KEY,
    BatchStartTime DATETIME NOT NULL DEFAULT GETDATE(),
    BatchEndTime DATETIME NULL,
    SourceSystem NVARCHAR(50) NOT NULL,
    RecordsExtracted INT NULL,
    RecordsTransformed INT NULL,
    RecordsLoaded INT NULL,
    Status NVARCHAR(20) NOT NULL DEFAULT 'Running',
    ErrorMessage NVARCHAR(MAX) NULL
);
GO

-- Create Data Load Log table
CREATE TABLE audit.DataLoadLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    BatchID INT NOT NULL FOREIGN KEY REFERENCES audit.ETLBatch(BatchID),
    TableName NVARCHAR(100) NOT NULL,
    LoadType NVARCHAR(20) NOT NULL,
    StartTime DATETIME NOT NULL DEFAULT GETDATE(),
    EndTime DATETIME NULL,
    RowsInserted INT NULL,
    RowsUpdated INT NULL,
    RowsDeleted INT NULL,
    Status NVARCHAR(20) NOT NULL DEFAULT 'Running',
    ErrorMessage NVARCHAR(MAX) NULL
);
GO

-- Create Error Log table
CREATE TABLE audit.ErrorLog (
    ErrorLogID INT IDENTITY(1,1) PRIMARY KEY,
    ErrorTime DATETIME NOT NULL DEFAULT GETDATE(),
    ErrorNumber INT NULL,
    ErrorSeverity INT NULL,
    ErrorState INT NULL,
    ErrorProcedure NVARCHAR(128) NULL,
    ErrorLine INT NULL,
    ErrorMessage NVARCHAR(4000) NULL,
    UserName NVARCHAR(128) NOT NULL DEFAULT SUSER_SNAME(),
    ApplicationName NVARCHAR(128) NULL,
    HostName NVARCHAR(128) NULL,
    AdditionalInfo NVARCHAR(MAX) NULL
);
GO

-- Create Data Quality Log table
CREATE TABLE audit.DataQualityLog (
    QualityLogID INT IDENTITY(1,1) PRIMARY KEY,
    BatchID INT NOT NULL FOREIGN KEY REFERENCES audit.ETLBatch(BatchID),
    CheckDate DATETIME NOT NULL DEFAULT GETDATE(),
    TableName NVARCHAR(100) NOT NULL,
    QualityCheckName NVARCHAR(100) NOT NULL,
    QualityCheckDescription NVARCHAR(255) NULL,
    RecordsTested INT NOT NULL,
    RecordsFailed INT NOT NULL,
    FailurePercentage DECIMAL(5,2) NULL,
    Severity NVARCHAR(20) NOT NULL DEFAULT 'Warning'
);
GO