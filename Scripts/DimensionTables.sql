USE BritishAirwaysDW;
GO

-- Create Date Dimension table
CREATE TABLE dim.Date (
    DateKey INT PRIMARY KEY,
    FullDate DATE NOT NULL,
    DayOfWeek TINYINT NOT NULL,
    DayName VARCHAR(10) NOT NULL,
    DayOfMonth TINYINT NOT NULL,
    DayOfYear SMALLINT NOT NULL,
    WeekOfYear TINYINT NOT NULL,
    MonthName VARCHAR(10) NOT NULL,
    MonthOfYear TINYINT NOT NULL,
    Quarter TINYINT NOT NULL,
    Year INT NOT NULL,
    IsWeekend BIT NOT NULL,
    IsHoliday BIT NOT NULL DEFAULT 0,
    HolidayName VARCHAR(50) NULL
);
GO

-- Create Author Dimension table
CREATE TABLE dim.Author (
    AuthorID INT IDENTITY(1,1) PRIMARY KEY,
    AuthorName NVARCHAR(100) NOT NULL,
    AuthorLocation NVARCHAR(100) NULL,
    CreatedDate DATETIME NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME NULL,
    IsActive BIT NOT NULL DEFAULT 1
);
GO

-- Create Flight Details Dimension table
CREATE TABLE dim.FlightDetails (
    FlightDetailID INT IDENTITY(1,1) PRIMARY KEY,
    SeatType NVARCHAR(50) NULL,
    Route NVARCHAR(255) NULL,
    TypeOfTraveller NVARCHAR(50) NULL,
    CreatedDate DATETIME NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME NULL,
    IsCurrent BIT NOT NULL DEFAULT 1
);
GO