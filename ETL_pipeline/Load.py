import pyodbc
import pandas as pd
from typing import Dict, Optional, Tuple
import logging
from contextlib import contextmanager

class DWConnection:
    """Data Warehouse connection handler with transaction support."""
    
    def __init__(self, server: str, database: str):
        self.server = server
        self.database = database
        self.connection_string = (
            f'DRIVER={{SQL Server}};SERVER={self.server};'
            f'DATABASE={self.database};Trusted_Connection=yes;'
        )
        self.connection = None
        self.logger = logging.getLogger('DWConnection')

    def connect(self):
        """Establish connection to the data warehouse."""
        try:
            self.connection = pyodbc.connect(self.connection_string)
            self.connection.autocommit = False
            self.logger.info("Successfully connected to the data warehouse")
            return True
        except pyodbc.Error as e:
            self.logger.error(f"Connection error: {e}")
            return False

    def close(self):
        """Close the data warehouse connection."""
        if self.connection:
            try:
                if self.connection.autocommit is False:
                    self.connection.rollback()
                self.connection.close()
                self.logger.info("Closed data warehouse connection")
            except pyodbc.Error as e:
                self.logger.error(f"Error closing connection: {e}")

    @contextmanager
    def transaction(self):
        """Context manager for handling transactions."""
        try:
            yield
            self.connection.commit()
            self.logger.info("Transaction committed successfully")
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Transaction rolled back due to error: {e}")
            raise

    def _get_dimension_map(self, table_name: str, key_col: str, value_cols):
        """Get mapping of dimension values to keys with proper NULL handling."""
        cursor = self.connection.cursor()
        
        if isinstance(value_cols, str):
            query = f"SELECT {value_cols}, {key_col} FROM dim.{table_name}"
            cursor.execute(query)
            return {
                (str(row[0]) if row[0] is not None else 'NULL'): row[1] 
                for row in cursor.fetchall()
            }
        else:
            cols_str = ', '.join(value_cols)
            query = f"SELECT {cols_str}, {key_col} FROM dim.{table_name}"
            cursor.execute(query)
            
            mapping = {}
            for row in cursor.fetchall():
                key_parts = []
                for i in range(len(value_cols)):
                    val = row[i]
                    key_parts.append(str(val) if val is not None else 'NULL')
                mapping[tuple(key_parts)] = row[-1]
            
            return mapping

    def _get_date_map(self):
        """Get date mapping with robust string-to-date conversion."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT FullDate, DateKey FROM dim.Date")
        
        date_map = {}
        for row in cursor.fetchall():
            if isinstance(row[0], str):
                # Handle string dates (from CSV)
                date_obj = pd.to_datetime(row[0]).date()
            else:
                # Handle date/datetime objects (from database)
                date_obj = row[0].date()
            date_map[date_obj] = row[1]
        
        return date_map

    def load_dimension(self, table_name: str, data: pd.DataFrame, key_column: str) -> Tuple[int, bool]:
        """Load data into a dimension table."""
        cursor = self.connection.cursor()
        insert_count = 0
        
        try:
            cursor.execute(f"SELECT {key_column} FROM dim.{table_name}")
            existing_keys = {row[0] for row in cursor.fetchall()}
            
            data['CreatedDate'] = pd.to_datetime('now')
            
            for _, row in data.iterrows():
                if row[key_column] not in existing_keys:
                    columns = ', '.join(row.index)
                    placeholders = ', '.join(['?'] * len(row))
                    query = f"INSERT INTO dim.{table_name} ({columns}) VALUES ({placeholders})"
                    cursor.execute(query, tuple(row))
                    insert_count += 1
            
            self.logger.info(f"Inserted {insert_count} new records into dim.{table_name}")
            return (insert_count, True)
            
        except pyodbc.Error as e:
            self.logger.error(f"Error loading dimension {table_name}: {e}")
            return (0, False)

    def load_fact_reviews(self, fact_data: pd.DataFrame, batch_id: int) -> Tuple[int, bool]:
        """Load review fact data with robust date handling."""
        cursor = self.connection.cursor()
        insert_count = 0
        missing_matches = 0
        
        try:
            # Get dimension mappings
            author_map = self._get_dimension_map('Author', 'AuthorID', 'AuthorName')
            flight_map = self._get_dimension_map('FlightDetails', 'FlightDetailID', 
                                            ['SeatType', 'Route', 'TypeOfTraveller'])
            date_map = self._get_date_map()
            
            # Process all records
            for _, row in fact_data.iterrows():
                # Author matching
                author_id = author_map.get(str(row['AuthorName']))
                if not author_id:
                    missing_matches += 1
                    continue
                
                # Flight details matching
                flight_key = (
                    str(row['SeatType']) if pd.notna(row['SeatType']) else 'NULL',
                    str(row['Route']) if pd.notna(row['Route']) else 'NULL',
                    str(row['TypeOfTraveller']) if pd.notna(row['TypeOfTraveller']) else 'NULL'
                )
                flight_id = flight_map.get(flight_key)
                if not flight_id:
                    missing_matches += 1
                    continue
                
                # Date handling with robust conversion
                try:
                    review_date = pd.to_datetime(row['ReviewDate']).date() if pd.notna(row['ReviewDate']) else None
                    review_date_key = date_map.get(review_date) if review_date else None
                    
                    flown_date = pd.to_datetime(row['DateFlown']).date() if pd.notna(row['DateFlown']) else None
                    flown_date_key = date_map.get(flown_date) if flown_date else None
                    
                    if not review_date_key:
                        missing_matches += 1
                        continue
                    
                    # Insert fact record
                    query = """
                    INSERT INTO fact.Reviews (
                        AuthorID, FlightDetailID, ReviewDateKey, DateFlownKey,
                        Rating, ReviewTitle, ReviewText, SeatComfort, 
                        CabinStaffService, FoodBeverages, InflightEntertainment,
                        GroundService, ValueForMoney, RecommendedService,
                        LoadDate, SourceSystem, BatchID
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = (
                        author_id, flight_id, review_date_key, flown_date_key,
                        float(row['Rating']), str(row['ReviewTitle']), str(row['ReviewText']), 
                        int(row['SeatComfort']) if pd.notna(row['SeatComfort']) else None, 
                        int(row['CabinStaffService']) if pd.notna(row['CabinStaffService']) else None,
                        int(row['FoodBeverages']) if pd.notna(row['FoodBeverages']) else None,
                        int(row['InflightEntertainment']) if pd.notna(row['InflightEntertainment']) else None,
                        int(row['GroundService']) if pd.notna(row['GroundService']) else None,
                        int(row['ValueForMoney']) if pd.notna(row['ValueForMoney']) else None,
                        str(row['RecommendedService']), 
                        pd.to_datetime('now'),
                        'WebScraper', 
                        batch_id
                    )
                    cursor.execute(query, params)
                    insert_count += 1
                    
                except Exception as e:
                    self.logger.warning(f"Skipping record due to date conversion error: {e}")
                    missing_matches += 1
                    continue
                
            if missing_matches > 0:
                self.logger.warning(f"Skipped {missing_matches} records due to matching issues")
            
            self.logger.info(f"Inserted {insert_count} fact records")
            return (insert_count, insert_count > 0)
            
        except pyodbc.Error as e:
            self.logger.error(f"Error loading fact data: {e}")
            return (0, False)

    def start_etl_batch(self, source_system: str) -> Optional[int]:
        """Start a new ETL batch."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO audit.ETLBatch (SourceSystem) 
                OUTPUT INSERTED.BatchID
                VALUES (?)
            """, source_system)
            batch_id = cursor.fetchone()[0]
            self.connection.commit()
            return batch_id
        except pyodbc.Error as e:
            self.logger.error(f"Error starting ETL batch: {e}")
            return None

    def complete_etl_batch(self, batch_id: int, status: str, records_loaded: int):
        """Complete an ETL batch with status."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                UPDATE audit.ETLBatch 
                SET BatchEndTime = GETDATE(), 
                    Status = ?,
                    RecordsLoaded = ?
                WHERE BatchID = ?
            """, (status, records_loaded, batch_id))
            self.connection.commit()
        except pyodbc.Error as e:
            self.logger.error(f"Error completing ETL batch: {e}")