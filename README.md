# Airline-Reviews-Data-Pipeline

## Problem Statement
The airline management finds it hard to get useful insights from customer feedback on different review sites. This makes it difficult to find common problems, measure how happy customers are with things like comfort, food, and service, track how performance changes over time, compare with other airlines, and make improvements. The main business problem is that executives want to know what causes low ratings on flights and which parts of the service have the biggest effect on overall customer satisfaction?

### Impact
Without systematic analysis of customer feedback, the airline:
- Misses the opportunities to improve customer satisfaction
- Lacks the visibility into service quality metrics
- Cannot measure ROI of service improvements
- Risks of losing customers to competitors with better-reviewed services

## MAIN HYPOTHESIS (Primary)
- Certain service aspects (seat comfort, staff service, food, entertainment, ground service) have a significant impact on overall customer satisfaction.

### SUPPORTING HYPOTHESES:
- Poor seat comfort is strongly associated with low overall ratings
- Certain flight routes consistently result in lower satisfaction scores
- Type of traveller (business, leisure, family) affects which service aspects matter most. For example, business travellers may value staff service more, families may value entertainment more.
- Food and beverages are a major driver of dissatisfaction on certain routes. Especially long-haul or international routes.


## DATA ELEMENTS
### Customer Feedback Data: To measure what is good or bad from the passenger’s perspective.
Data Elements:
- Customer overall rating of the flight
- Ratings of specific service areas:
 - seat comfort
 - cabin staff service
 - food & beverages
 - inflight entertainment
 - ground service
 - value for money
- The customer’s full written review
- Review title or summary
- Whether the customer recommends the flight


### Time & Seasonal Context: 
To identify when service quality drops, improves, or is inconsistent. These help explain service trends over time.
Data Elements:
- Review date
- Month, quarter, and year
- Weekday vs weekend
- Holiday period indicator
- Season (peak travel, off-season)

### Sentiment & Text Insights: 
To detect hidden issues not captured by numeric ratings (e.g., “rude staff”, “flight delay”, “broken seats”). These elements come from running NLP on the review text.
Data Elements:
-	Positive sentiment score
-	Negative sentiment score
-	Overall sentiment (compound score)
-	Key phrases mentioned by customers
-	Entities mentioned (food, staff, delay, aircraft, etc.)

### Reviewer / Passenger Information: 
Different groups have different expectations and experience problems differently. This is information about who is experiencing the problems.
Data Elements:
- Reviewer ID
- Reviewer location (country)
- Reviewer type (optional: frequent flyer, first-time flyer)
- Reliability of the reviewer (active or inactive profile)


### Flight Operational Details: 
To know where and under what conditions problems occur. These describe the actual flight context.
Data Elements:
- Route (origin to destination)
- Seat type (economy, business, first class)
- Type of traveller (leisure, business, family, solo)
- Flight date
- Service type (long haul vs short haul)


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

### Star Schema
<img width="947" height="884" alt="Reviews Star Schema" src="https://github.com/user-attachments/assets/fc443f24-d977-49df-9ab2-f5438bc62f69" />

## Data Analytics Dash Board Images
### Aircraft Customer Satisfaction Summary: Overall Overview
<img width="1410" height="787" alt="image" src="https://github.com/user-attachments/assets/c13c0f99-e09a-40ee-9b18-e6f8888b3110" />

### Risk Arlet For Services
<img width="1407" height="784" alt="image" src="https://github.com/user-attachments/assets/a50526df-7308-43c3-9e61-ca3af3a8ab7b" />

### Route Performance Analysis (In-progress)
<img width="1409" height="789" alt="image" src="https://github.com/user-attachments/assets/ca7a1f07-1972-498b-b9ad-101edb83e6ec" />

### Traveller Type Service 
<img width="1402" height="748" alt="image" src="https://github.com/user-attachments/assets/07225b7b-75a5-4313-9ca7-44fed35dbdf0" />


### Recommendation Service Analysis (In-progress)

### Service Performance Analysis (In-progress)

### KPis (In-progress)

## Statistics Visualisations
### Distributions
<img width="1411" height="775" alt="image" src="https://github.com/user-attachments/assets/2c749e24-e38b-48dd-94ce-a7ba5c1fcf27" />

### Probability Distributions
<img width="1410" height="786" alt="image" src="https://github.com/user-attachments/assets/c2939eac-109b-4b01-9f4f-8ae6f54a58b2" />


### Normal Distribution
<img width="1414" height="791" alt="image" src="https://github.com/user-attachments/assets/8fbb325e-925a-4ecb-914d-5ec1d92ab92c" />
<img width="1408" height="788" alt="image" src="https://github.com/user-attachments/assets/e6f3244d-36cd-4518-ab52-9fdc121da0e8" />

### Bayesian Model
<img width="1406" height="780" alt="image" src="https://github.com/user-attachments/assets/6518e702-a02c-43d6-aa83-883442252593" />



## PREDECTIVE MODELS AND RECOMMENDATION SYSTEM MODEL (In-progress)

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
