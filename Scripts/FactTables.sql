USE BritishAirwaysDW;
GO

-- Create Review Fact table
CREATE TABLE fact.Reviews (
    ReviewID INT IDENTITY(1,1) PRIMARY KEY,
    AuthorID INT NOT NULL FOREIGN KEY REFERENCES dim.Author(AuthorID),
    FlightDetailID INT NOT NULL FOREIGN KEY REFERENCES dim.FlightDetails(FlightDetailID),
    ReviewDateKey INT NOT NULL FOREIGN KEY REFERENCES dim.Date(DateKey),
    DateFlownKey INT NULL FOREIGN KEY REFERENCES dim.Date(DateKey),
    Rating DECIMAL(3,1) NOT NULL,
    ReviewTitle NVARCHAR(255) NOT NULL,
    ReviewText NVARCHAR(MAX) NOT NULL,
    SeatComfort TINYINT NULL,
    CabinStaffService TINYINT NULL,
    FoodBeverages TINYINT NULL,
    InflightEntertainment TINYINT NULL,
    GroundService TINYINT NULL,
    ValueForMoney TINYINT NULL,
    RecommendedService NVARCHAR(10) NOT NULL,
    LoadDate DATETIME NOT NULL DEFAULT GETDATE(),
    SourceSystem NVARCHAR(50) NOT NULL DEFAULT 'WebScraper',
    BatchID INT NOT NULL
);
GO

-- Create Review Sentiment Analysis table
CREATE TABLE fact.ReviewSentiment (
    SentimentID INT IDENTITY(1,1) PRIMARY KEY,
    ReviewID INT NOT NULL FOREIGN KEY REFERENCES fact.Reviews(ReviewID),
    PositiveScore DECIMAL(5,2) NULL,
    NegativeScore DECIMAL(5,2) NULL,
    NeutralScore DECIMAL(5,2) NULL,
    CompoundScore DECIMAL(5,2) NULL,
    KeyPhrases NVARCHAR(MAX) NULL,
    Entities NVARCHAR(MAX) NULL,
    AnalysisDate DATETIME NOT NULL DEFAULT GETDATE(),
    ModelVersion NVARCHAR(20) NOT NULL
);
GO