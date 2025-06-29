# Airline-Reviews-Data-Pipeline

## Problem Statement
The airline management lacks actionable insights from customer feedback that is spread across different review sites, making it difficult to:
- Identify recurring pain points in customer experience
- Quantify satisfaction across service dimensions (comfort, food, service)
- Track performance trends over time
- Benchmark against competitor airlines
- Support data-driven operational improvements to enhance overall service quality.

### Impact
Without systematic analysis of customer feedback, the airline:
- Misses the opportunities to improve customer satisfaction
- Lacks the visibility into service quality metrics
- Cannot measure ROI of service improvements
- Risks of losing customers to competitors with better-reviewed services

## Data Engineering Solution
Web Scraping |------> Data Cleaning |------> Dimensional Modeling |------> Data Warehouse |------> Analytics

Solution:
- Automated Data Collection: Scrapes reviews with metadata (ratings, dates, traveler types)
- Structured Storage: Implements a star schema data warehouse for analytics
- Quality Assurance: Data validation and cleaning at each pipeline stage
- Historical Tracking: Preserves review history for trend analysis

## Data Model
### Star Schema Design:
- Fact Table: fact.Reviews, fact.ReviewSentiment
- Dimension Table: dim.Author, dim.FlightDetails, dim.Date

## Other Tables 
### Audit log
- audit.DataLoadLog
- audit.DataQualityLog
- audit.ErrorLog
- audit.ETLBatch


## Technical Implementation
### Technologies Used:
- Python (BeautifulSoup, Pandas, pyodbc)
- Microsoft SQL Server
- Docker
- Apache airflow Scheduler (for automation)
- CSV files

### Key Features:
- Transactional loading (all-or-nothing integrity)
- Comprehensive error handling and logging
- Slowly Changing Dimensions for historical tracking
- Audit trail of all ETL processes