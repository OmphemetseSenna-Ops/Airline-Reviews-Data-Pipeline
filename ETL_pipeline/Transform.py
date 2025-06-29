import pandas as pd
import os
from datetime import datetime
from typing import Dict
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DataProcessing')

def clean_review_data(reviews_df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform review data for DW loading."""
    logger.info("Starting data cleaning process")
    
    # Convert ratings to numeric
    rating_columns = [
        'Rating', 'SeatComfort', 'CabinStaffService', 
        'FoodBeverages', 'InflightEntertainment', 
        'GroundService', 'ValueForMoney'
    ]
    
    for column in rating_columns:
        reviews_df[column] = pd.to_numeric(reviews_df[column], errors='coerce')
        reviews_df[column] = reviews_df[column].fillna(0)
        logger.debug(f"Processed ratings for {column}")

    # Convert dates to datetime with error handling
    reviews_df['ReviewDate'] = pd.to_datetime(reviews_df['ReviewDate'], errors='coerce')
    
    # Special handling for DateFlown which might be in "Month Year" format
    def parse_date_flown(date_str):
        try:
            if pd.isna(date_str):
                return pd.NaT
            if isinstance(date_str, str):
                if len(date_str.split()) == 2:  #"Month Year" format
                    return pd.to_datetime(date_str, format='%B %Y', errors='coerce')
            return pd.to_datetime(date_str, errors='coerce')
        except Exception:
            return pd.NaT
    
    reviews_df['DateFlown'] = reviews_df['DateFlown'].apply(parse_date_flown)
    
    logger.debug("Completed date conversions")

    # Clean text fields
    text_columns = ['ReviewTitle', 'AuthorLocation', 'ReviewText']
    for col in text_columns:
        reviews_df[col] = reviews_df[col].astype(str)
    
    reviews_df['ReviewTitle'] = reviews_df['ReviewTitle'].str.replace('["“”]', '', regex=True)
    reviews_df['AuthorLocation'] = reviews_df['AuthorLocation'].str.replace(r'[()]', '', regex=True)
    
    # Handle recommended field
    reviews_df['RecommendedService'] = reviews_df['RecommendedService'].str.upper()
    reviews_df['RecommendedService'] = reviews_df['RecommendedService'].replace({
        'YES': 'YES',
        'NO': 'NO',
        'Y': 'YES',
        'N': 'NO'
    }).fillna('NO')
    
    logger.debug("Completed text cleaning")

    # Drop duplicates and invalid dates
    initial_count = len(reviews_df)
    reviews_df.drop_duplicates(
        subset=['AuthorName', 'ReviewText'], 
        keep='first', 
        inplace=True
    )
    reviews_df = reviews_df.dropna(subset=['ReviewDate', 'DateFlown'])
    final_count = len(reviews_df)
    
    logger.info(f"Removed {initial_count - final_count} invalid/duplicate records")

    # Ensure dates are within the reasonable range
    current_year = pd.Timestamp.now().year
    date_mask = (
        (reviews_df['ReviewDate'].dt.year >= 2000) & 
        (reviews_df['ReviewDate'].dt.year <= current_year + 1) &
        (reviews_df['DateFlown'].dt.year >= 2000) & 
        (reviews_df['DateFlown'].dt.year <= current_year + 1)
    )
    reviews_df = reviews_df[date_mask]
    
    logger.info(f"Final cleaned dataset contains {len(reviews_df)} records")
    return reviews_df

def prepare_dw_load_data(cleaned_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Prepare data for DW loading by creating appropriate structures."""
    logger.info("Preparing data for DW loading")
    
    # Create Author dimension data
    author_dim = cleaned_df[['AuthorName', 'AuthorLocation']].copy()
    author_dim = author_dim.drop_duplicates()
    author_dim['CreatedDate'] = datetime.now()
    author_dim['IsActive'] = True
    
    logger.info(f"Prepared {len(author_dim)} author dimension records")

    # Create FlightDetails dimension data
    flight_dim = cleaned_df[['SeatType', 'Route', 'TypeOfTraveller']].copy()
    flight_dim = flight_dim.drop_duplicates()
    flight_dim['CreatedDate'] = datetime.now()
    flight_dim['IsCurrent'] = True
    
    logger.info(f"Prepared {len(flight_dim)} flight details dimension records")

    # Prepare fact data with references
    fact_data = cleaned_df.copy()
    
    return {
        'author_dim': author_dim,
        'flight_dim': flight_dim,
        'review_fact': fact_data
    }

def save_intermediate_data(data_dict: Dict[str, pd.DataFrame], output_folder: str = 'output'):
    """Save intermediate data to CSV files."""
    logger.info(f"Saving intermediate data to {output_folder}")
    os.makedirs(output_folder, exist_ok=True)
    
    for key, df in data_dict.items():
        file_path = os.path.join(output_folder, f'{key}.csv')
        try:
            df.to_csv(file_path, index=False)
            logger.info(f"Saved {len(df)} records to {file_path}")
        except Exception as e:
            logger.error(f"Error saving {key} to CSV: {e}")
            raise