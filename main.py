import pandas as pd
from typing import Dict
from ETL_pipeline.Extract import check_connection, fetch_reviews
from ETL_pipeline.Transform import clean_review_data, prepare_dw_load_data, save_intermediate_data
from ETL_pipeline.Load import DWConnection
import time
import logging
import sys


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

def scrape_reviews(pages: int = 7, offset: int = 100) -> pd.DataFrame:
    """Scrape reviews from the website."""
    url = 'https://www.airlinequality.com/airline-reviews/ethiopian-airlines/'
    if not check_connection(url):
        logging.error("Failed to connect to the URL. Please check your connection.")
        return pd.DataFrame()

    all_reviews = []
    for i in range(1, pages + 1):
        reviews_data = fetch_reviews(i, offset)
        all_reviews.extend(reviews_data)
        time.sleep(2)

    return pd.DataFrame(all_reviews)

def verify_date_dimension(dw: DWConnection) -> bool:
    """Verify date dimension is populated."""
    try:
        cursor = dw.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM dim.Date")
        date_count = cursor.fetchone()[0]
        if date_count == 0:
            logging.error("Date dimension is empty! Please run the SQL script to populate dim.Date")
            return False
        logging.info(f"Verified date dimension contains {date_count} records")
        return True
    except pyodbc.Error as e:
        logging.error(f"Error verifying date dimension: {e}")
        return False

def load_to_dw(data_dict: Dict[str, pd.DataFrame], server: str, database: str) -> bool:
    """Load data into DW with transaction support."""
    dw = DWConnection(server, database)
    if not dw.connect():
        return False

    try:
        # Verify date dimension first
        if not verify_date_dimension(dw):
            return False

        # Start ETL batch
        batch_id = dw.start_etl_batch('WebScraper')
        if batch_id is None:
            logging.error("Failed to start ETL batch")
            return False
        logging.info(f"Started ETL batch with ID: {batch_id}")

        # Process in transaction
        with dw.transaction():
            # Load dimensions
            author_count, author_success = dw.load_dimension('Author', data_dict['author_dim'], 'AuthorName')
            flight_count, flight_success = dw.load_dimension('FlightDetails', data_dict['flight_dim'], 'Route')
            
            if not all([author_success, flight_success]):
                raise Exception("Dimension loading failed")
            
            if author_count == 0 or flight_count == 0:
                raise Exception("No dimension records loaded")
            
            # Load facts
            fact_count, fact_success = dw.load_fact_reviews(data_dict['review_fact'], batch_id)
            if not fact_success or fact_count == 0:
                raise Exception("Fact loading failed")
            
            # Mark complete
            dw.complete_etl_batch(batch_id, 'Completed', fact_count)
            logging.info(f"Successfully loaded {fact_count} fact records")
            return True

    except Exception as e:
        logging.error(f"Error during DW loading: {e}")
        try:
            if 'batch_id' in locals():
                dw.complete_etl_batch(batch_id, 'Failed', 0)
        except Exception as inner_e:
            logging.error(f"Error marking batch as failed: {inner_e}")
        return False
    finally:
        dw.close()

def main():
    # Configuration
    SERVER = 'localhost'
    DATABASE = 'BritishAirwaysDW'
    PAGES_TO_SCRAPE = 7
    REVIEWS_PER_PAGE = 100

    # Step 1: Scrape reviews
    logging.info("Starting review scraping process")
    raw_reviews = scrape_reviews(PAGES_TO_SCRAPE, REVIEWS_PER_PAGE)
    
    if raw_reviews.empty:
        logging.error("No reviews were scraped. Exiting.")
        return

    # Step 2: Clean and process data
    logging.info("Cleaning and processing scraped data")
    cleaned_data = clean_review_data(raw_reviews)
    dw_data = prepare_dw_load_data(cleaned_data)
    
    # Save intermediate files
    save_intermediate_data(dw_data)
    
    # Step 3: Load to data warehouse
    logging.info("Loading data into data warehouse")
    success = load_to_dw(dw_data, SERVER, DATABASE)
    
    if success:
        logging.info("ETL process completed successfully")
    else:
        logging.error("ETL process failed")

if __name__ == "__main__":
    main()