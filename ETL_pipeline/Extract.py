import requests
from bs4 import BeautifulSoup
import time
import pandas as pd

def check_connection(url):
    """Check connection to the specified URL."""
    try:
        response = requests.head(url)
        return response.status_code == 200
    except requests.ConnectionError:
        return False

def fetch_reviews(page_number, offset):
    """Fetch reviews from airline quality website."""
    url = f"https://www.airlinequality.com/airline-reviews/ethiopian-airlines/page/{page_number}/?sortby=post_date%3ADesc&pagesize={offset}"
    
    print(f"Fetching reviews from page {page_number} with offset: {offset}.")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        reviews = soup.find_all('article', itemprop='review')

        if not reviews:
            print("No reviews found on this page.")
            return []

        data = []
        for review in reviews:
            # Extract review rating
            rating = review.find('span', itemprop='ratingValue')
            rating = rating.text.strip() if rating else "0"
            
            # Extract author information
            author = review.find('span', itemprop='name')
            author = author.text.strip() if author else "Anonymous"
            
            # Extract review heading
            heading_service = review.find('h2', class_='text_header')
            heading_service = heading_service.text.strip() if heading_service else "No Title"
            
            # Extract author location
            author_span = review.find('span', itemprop='author')
            if author_span and author_span.next_sibling:
                location_text = author_span.next_sibling.strip()
                location_text = location_text.replace('(', '').replace(')', '').strip()
            else:
                location_text = "Unknown"
            
            # Extract review date
            date = review.find('time', itemprop='datePublished')
            date = date['datetime'] if date else None
            
            # Extract review text
            review_body = review.find('div', class_='text_content', itemprop='reviewBody')
            review_body = review_body.text.strip() if review_body else "No Content"
            
            # Initialize additional review details
            type_traveller = "Unknown"
            date_flown = None
            seat_type = "Unknown"
            route = "Unknown"
            recommended = "No"
            
            # Initialize rating fields
            seat_comfort = 0
            cabin_staff_service = 0
            food_rating = 0
            entertainment_rating = 0
            ground_service_rating = 0
            value_for_money = 0

            # Extract review statistics
            stats = review.find('div', class_='review-stats')
            if stats:
                rows = stats.find_all('tr')
                for row in rows:
                    header = row.find('td', class_='review-rating-header')
                    value = row.find('td', class_='review-value')
                    stars = row.find('td', class_='review-rating-stars')
                    
                    if header:
                        header_text = header.text.strip().lower()
                        
                        # Handle text-based review attributes
                        if "type of traveller" in header_text:
                            type_traveller = value.text.strip() if value else "Unknown"
                        elif "seat type" in header_text:
                            seat_type = value.text.strip() if value else "Unknown"
                        elif "route" in header_text:
                            route = value.text.strip() if value else "Unknown"
                        elif "recommended" in header_text:
                            recommended = value.text.strip() if value else "No"
                        elif "date flown" in header_text:
                            date_flown = value.text.strip() if value else None
                        
                        # Handle star ratings
                        if stars:
                            star_count = len(stars.find_all('span', class_='star fill'))
                            if "seat comfort" in header_text:
                                seat_comfort = star_count
                            elif "cabin staff service" in header_text:
                                cabin_staff_service = star_count
                            elif "food & beverages" in header_text:
                                food_rating = star_count
                            elif "inflight entertainment" in header_text:
                                entertainment_rating = star_count
                            elif "ground service" in header_text:
                                ground_service_rating = star_count
                            elif "value for money" in header_text:
                                value_for_money = star_count

            # Append the extracted data with DW field names
            data.append({
                'AuthorName': author,
                'AuthorLocation': location_text,
                'ReviewDate': date,
                'ReviewTitle': heading_service,
                'ReviewText': review_body,
                'TypeOfTraveller': type_traveller,
                'SeatType': seat_type,
                'Route': route,
                'DateFlown': date_flown,
                'Rating': float(rating) if rating.replace('.', '', 1).isdigit() else 0.0,
                'SeatComfort': seat_comfort,
                'CabinStaffService': cabin_staff_service,
                'FoodBeverages': food_rating,
                'InflightEntertainment': entertainment_rating,
                'GroundService': ground_service_rating,
                'ValueForMoney': value_for_money,
                'RecommendedService': 'YES' if recommended.lower() == 'yes' else 'NO'
            })

        return data

    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page_number}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error processing page {page_number}: {e}")
        return []