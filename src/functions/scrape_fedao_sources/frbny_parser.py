#!/usr/bin/env python3
"""
Complete FRBNY Scraper - Fixed Release Date Extraction
Web First, PDF Fallback (Pipeline Compatible)
Fixed to extract release date from first <td> element only
"""

import os
import re
import sys
import csv
import time
import pdfplumber
import pandas as pd
import requests
import io
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin
import argparse
import logging

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CombinedFRBNYScraper:
    """Complete scraper for FRBNY data - Web first, PDF fallback with FIXED release date extraction"""
    
    def __init__(self):
        self.data = []
        self.source_type = None  # 'web' or 'pdf'
        
    def setup_driver(self):
        """Setup Chrome driver with appropriate options"""
        chrome_options = Options()
        
        # Check if running in cloud environment
        if os.environ.get('GOOGLE_CLOUD_PROJECT') or os.environ.get('FUNCTIONS_FRAMEWORK_VERSION'):
            # Cloud Function environment - keep headless for production
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--single-process")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            chrome_options.binary_location = os.environ.get('CHROME_BIN', '/opt/chrome/chrome')
        else:
            # Local environment - can run without headless for debugging
            # Uncomment next line for debugging: chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
        
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Set timeouts
            driver.set_page_load_timeout(60)
            driver.implicitly_wait(10)
            
            return driver
        except Exception as e:
            logger.error(f"Failed to setup Chrome driver: {e}")
            return None

    def parse_maximum_operation_size(self, max_op_size):
        """Parse the Maximum Operation Size into currency, size, and multiplier"""
        logger.debug(f"Parsing: '{max_op_size}'")
        
        if not max_op_size or max_op_size.strip() == "":
            return "", "", ""
        
        # Clean the string
        max_op_size = max_op_size.strip()
        
        # Extract currency (usually $ but could be others)
        currency_match = re.match(r'^([^\d\s]+)', max_op_size)
        currency = currency_match.group(1) if currency_match else ""
        
        # Extract the numeric value and multiplier
        pattern = r'([0-9.,]+)\s*(million|billion|trillion|thousand)?'
        match = re.search(pattern, max_op_size.lower())
        
        if match:
            size = match.group(1)
            multiplier = match.group(2) if match.group(2) else ""
        else:
            size = ""
            multiplier = ""
        
        # Convert size to numeric if possible
        try:
            if size:
                size_float = float(size.replace(',', ''))
                if size_float.is_integer():
                    size = int(size_float)
                else:
                    size = size_float
        except ValueError:
            pass
        
        result = (currency, size, multiplier)
        logger.debug(f"Parsed result: {result}")
        return result

    def extract_release_date_from_web_simple(self, base_url):
        """FIXED: Extract release date focusing ONLY on the first <td> element in the table"""
        logger.info("🌐 Extracting release date from first TD element only...")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache'
        }
        
        max_attempts = 5
        base_wait_time = 30  # 30 seconds base wait time
        
        for attempt in range(max_attempts):
            try:
                if attempt == 0:
                    logger.info("🔍 Attempt 1/5: Immediate request...")
                else:
                    wait_time = base_wait_time + (attempt - 1) * 15  # 30, 45, 60, 75 seconds
                    logger.info(f"⏰ Retry attempt {attempt + 1}/5, waiting {wait_time} seconds for table content to load...")
                    time.sleep(wait_time)
                
                logger.info(f"📡 Making HTTP request to: {base_url}")
                response = requests.get(base_url, headers=headers, timeout=60)
                response.raise_for_status()
                
                page_content = response.text
                logger.info(f"📄 Received {len(page_content)} characters of content")
                
                # STRATEGY: Focus ONLY on the first <td> in the table structure
                logger.info("🎯 Looking specifically for first <td> element in pagination table...")
                
                # Look for the table structure with id="pagination-table"
                table_pattern = r'<table[^>]*id=["\']pagination-table["\'][^>]*>.*?<tbody[^>]*id=["\']data-container["\'][^>]*>(.*?)</tbody>'
                table_match = re.search(table_pattern, page_content, re.DOTALL | re.IGNORECASE)
                
                if table_match:
                    tbody_content = table_match.group(1)
                    logger.info("✅ Found pagination table with data-container tbody")
                    
                    # Look for the first <tr> and then the first <td>
                    first_row_pattern = r'<tr[^>]*>\s*<td[^>]*>(.*?)</td>'
                    first_td_match = re.search(first_row_pattern, tbody_content, re.DOTALL | re.IGNORECASE)
                    
                    if first_td_match:
                        first_td_content = first_td_match.group(1)
                        logger.info(f"🎯 Found first TD content: '{first_td_content}'")
                        
                        # Clean up the content (remove HTML tags, normalize whitespace)
                        clean_content = re.sub(r'<[^>]+>', ' ', first_td_content)  # Remove HTML tags
                        clean_content = re.sub(r'\s+', ' ', clean_content.strip())  # Normalize whitespace
                        logger.info(f"🧹 Cleaned TD content: '{clean_content}'")
                        
                        # Look for date pattern in the cleaned content
                        # Pattern: "6/13/2025 - 7/14/2025" or "6/13/2025 -<br>7/14/2025"
                        date_pattern = r'(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})'
                        date_match = re.search(date_pattern, clean_content)
                        
                        if date_match:
                            start_date = date_match.group(1)
                            end_date = date_match.group(2)
                            
                            logger.info(f"📅 Found operation period in first TD: {start_date} - {end_date}")
                            
                            # Validate the dates
                            try:
                                start_obj = datetime.strptime(start_date, '%m/%d/%Y')
                                end_obj = datetime.strptime(end_date, '%m/%d/%Y')
                                
                                current_year = datetime.now().year
                                
                                # Ensure dates are current year or later
                                if start_obj.year >= current_year and end_obj.year >= current_year:
                                    # Use the end date as release date
                                    release_date = str(int(end_obj.strftime('%Y%m%d')))
                                    
                                    logger.info(f"✅ SUCCESS on attempt {attempt + 1}!")
                                    logger.info(f"📅 Operation period: {start_date} - {end_date}")
                                    logger.info(f"🎯 Using end date as release date: {end_date} -> {release_date}")
                                    
                                    return release_date
                                else:
                                    logger.warning(f"⚠️  Dates are from old year: {start_obj.year}-{end_obj.year}")
                            
                            except ValueError as e:
                                logger.warning(f"⚠️  Could not parse dates: {e}")
                        else:
                            logger.warning(f"⚠️  No date pattern found in first TD content: '{clean_content}'")
                    else:
                        logger.warning("⚠️  Could not find first <td> element in table")
                else:
                    logger.warning("⚠️  Could not find pagination table with data-container")
                    
                    # Fallback: Look for ANY table with the expected structure
                    logger.info("🔄 Fallback: Looking for any table with period data...")
                    fallback_pattern = r'<td[^>]*>([^<]*\d{1,2}/\d{1,2}/\d{4}[^<]*-[^<]*\d{1,2}/\d{1,2}/\d{4}[^<]*)</td>'
                    fallback_matches = re.findall(fallback_pattern, page_content, re.IGNORECASE)
                    
                    if fallback_matches:
                        logger.info(f"🔄 Found {len(fallback_matches)} potential period TDs")
                        
                        # Take the first match (should be most recent)
                        first_match = fallback_matches[0]
                        clean_match = re.sub(r'<[^>]+>', ' ', first_match).strip()
                        clean_match = re.sub(r'\s+', ' ', clean_match)
                        
                        logger.info(f"🔄 First fallback match: '{clean_match}'")
                        
                        date_pattern = r'(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})'
                        date_match = re.search(date_pattern, clean_match)
                        
                        if date_match:
                            start_date = date_match.group(1)
                            end_date = date_match.group(2)
                            
                            try:
                                end_obj = datetime.strptime(end_date, '%m/%d/%Y')
                                current_year = datetime.now().year
                                
                                if end_obj.year >= current_year:
                                    release_date = str(int(end_obj.strftime('%Y%m%d')))
                                    logger.info(f"✅ FALLBACK SUCCESS on attempt {attempt + 1}: {start_date} - {end_date} -> {release_date}")
                                    return release_date
                            except ValueError:
                                pass
                
                # If we get here, this attempt failed
                if attempt < max_attempts - 1:
                    logger.warning(f"❌ Attempt {attempt + 1} failed, will retry with longer wait...")
                    continue
                else:
                    logger.error(f"❌ Final attempt {attempt + 1} failed")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"⏱️  Attempt {attempt + 1} timed out after 60 seconds")
                continue
            except requests.exceptions.RequestException as e:
                logger.warning(f"📡 Attempt {attempt + 1} request failed: {e}")
                continue
            except Exception as e:
                logger.warning(f"❌ Attempt {attempt + 1} failed with error: {e}")
                continue
        
        # If all attempts failed
        logger.error(f"❌ All {max_attempts} attempts failed to extract release date from first TD")
        logger.error("💡 Expected format: <td>6/13/2025 -<br>7/14/2025</td>")
        return None

    def extract_release_date_with_beautifulsoup(self, base_url):
        """Alternative method using BeautifulSoup for more reliable HTML parsing"""
        try:
            from bs4 import BeautifulSoup
            
            logger.info("🍲 Using BeautifulSoup for HTML parsing...")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            # Wait 30 seconds before making request to allow content to load
            logger.info("⏰ Waiting 30 seconds for content to load...")
            time.sleep(30)
            
            response = requests.get(base_url, headers=headers, timeout=60)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for the pagination table
            pagination_table = soup.find('table', {'id': 'pagination-table'})
            
            if pagination_table:
                # Find the data container tbody
                data_container = pagination_table.find('tbody', {'id': 'data-container'})
                
                if data_container:
                    # Get the first row
                    first_row = data_container.find('tr')
                    
                    if first_row:
                        # Get the first cell
                        first_cell = first_row.find('td')
                        
                        if first_cell:
                            cell_text = first_cell.get_text(strip=True)
                            logger.info(f"🎯 First cell text: '{cell_text}'")
                            
                            # Extract date pattern
                            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})', cell_text)
                            
                            if date_match:
                                start_date = date_match.group(1)
                                end_date = date_match.group(2)
                                
                                end_obj = datetime.strptime(end_date, '%m/%d/%Y')
                                release_date = str(int(end_obj.strftime('%Y%m%d')))
                                
                                logger.info(f"✅ BeautifulSoup SUCCESS: {start_date} - {end_date} -> {release_date}")
                                return release_date
            
            logger.warning("❌ BeautifulSoup method failed to find expected structure")
            return None
            
        except ImportError:
            logger.warning("⚠️  BeautifulSoup not available, skipping this method")
            return None
        except Exception as e:
            logger.warning(f"❌ BeautifulSoup method failed: {e}")
            return None

    def _validate_and_convert_date_pair(self, start_date_str, end_date_str, attempt_num):
        """Helper method to validate a date pair with enhanced checks"""
        try:
            start_obj = datetime.strptime(start_date_str, '%m/%d/%Y')
            end_obj = datetime.strptime(end_date_str, '%m/%d/%Y')
            
            current_year = datetime.now().year
            
            # Both dates should be current year or later
            if start_obj.year >= current_year and end_obj.year >= current_year:
                # End date should be after start date
                if end_obj >= start_obj:
                    # Date range should be reasonable (not too long)
                    days_diff = (end_obj - start_obj).days
                    if 1 <= days_diff <= 365:  # Between 1 day and 1 year
                        logger.info(f"✅ Valid date pair: {start_date_str} - {end_date_str} ({days_diff} days)")
                        return True
                    else:
                        logger.debug(f"❌ Date range too long: {days_diff} days")
                else:
                    logger.debug(f"❌ End date before start date: {start_date_str} > {end_date_str}")
            else:
                logger.debug(f"❌ Old dates: {start_obj.year}, {end_obj.year} (need >= {current_year})")
            
            return False
            
        except ValueError as e:
            logger.debug(f"❌ Date parsing failed: {e}")
            return False

    def _is_reasonable_release_date(self, release_date_str):
        """Helper method to check if release date is reasonable with enhanced validation"""
        try:
            date_obj = datetime.strptime(release_date_str, '%Y%m%d')
            today = datetime.now()
            
            # Should be within reasonable range
            min_date = datetime(today.year - 1, 1, 1)  # Not older than last year
            max_date = datetime(today.year + 2, 12, 31)  # Not more than 2 years future
            
            if min_date <= date_obj <= max_date:
                # Additional check: not too far in the past or future from today
                days_diff = abs((date_obj - today).days)
                if days_diff <= 365:  # Within 1 year of today
                    logger.debug(f"✅ Release date {release_date_str} is reasonable ({days_diff} days from today)")
                    return True
                else:
                    logger.warning(f"⚠️  Release date {release_date_str} is {days_diff} days from today (seems far)")
                    return True  # Still accept but warn
            else:
                logger.warning(f"❌ Date {release_date_str} outside reasonable range {min_date.strftime('%Y%m%d')} - {max_date.strftime('%Y%m%d')}")
                return False
                
        except Exception as e:
            logger.warning(f"❌ Date validation failed for {release_date_str}: {e}")
            return False

    def calculate_dataset_release_date(self, operations: List[Dict], driver=None) -> str:
        """FIXED: Calculate release date using the improved extraction method"""
        base_url = "https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details"
        
        # Try the improved web extraction first
        release_date = self.extract_release_date_from_web_simple(base_url)
        
        if release_date and self._is_reasonable_release_date(release_date):
            logger.info(f"✅ Using web-extracted release date: {release_date}")
            return release_date
        
        # Try BeautifulSoup as secondary method
        logger.info("🔄 Trying BeautifulSoup method...")
        release_date = self.extract_release_date_with_beautifulsoup(base_url)
        
        if release_date and self._is_reasonable_release_date(release_date):
            logger.info(f"✅ Using BeautifulSoup-extracted release date: {release_date}")
            return release_date
        
        # Fallback 1: Try to extract from browser if driver is available
        if driver:
            try:
                browser_release_date = self.extract_operation_period_end_date(driver)
                if browser_release_date and self._is_reasonable_release_date(browser_release_date):
                    logger.info(f"✅ Using browser-extracted release date: {browser_release_date}")
                    return browser_release_date
            except Exception as e:
                logger.warning(f"Browser extraction failed: {e}")
        
        # Fallback 2: Use maximum date from operations (original logic)
        logger.warning("All extraction methods failed, using fallback method...")
        max_date = None
        for operation in operations:
            for date_field in ['OPERATION DATE', 'SETTLEMENT DATE']:
                date_str = operation.get(date_field, '')
                if date_str:
                    try:
                        date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                        if max_date is None or date_obj > max_date:
                            max_date = date_obj
                    except ValueError:
                        continue
        
        if max_date:
            release_date = str(int(max_date.strftime('%Y%m%d')))
            logger.info(f"✅ Using maximum operation date as release date: {release_date}")
            return release_date
        else:
            # Final fallback: current date
            fallback_date = str(int(datetime.now().strftime('%Y%m%d')))
            logger.warning(f"⚠️  No valid dates found anywhere, using current date: {fallback_date}")
            return fallback_date

    def apply_release_date_to_operations(self, operations: List[Dict], driver=None) -> List[Dict]:
        """UPDATED: Apply the calculated release date using improved extraction"""
        if not operations:
            return operations
        
        # Calculate the release date using the improved method
        dataset_release_date = self.calculate_dataset_release_date(operations, driver)
        
        # Apply to all operations
        for operation in operations:
            operation['release_date'] = int(dataset_release_date)
        
        logger.info(f"✅ Applied release date {dataset_release_date} to {len(operations)} operations")
        return operations

    def extract_operation_period_end_date(self, driver=None) -> str:
        """Extract the operation period end date from the Operation Period Details tab"""
        release_date = None
        
        if driver:
            try:
                # Try to find the operation period details tab and extract the period
                operation_period_tab = driver.find_element(By.ID, "tab1")
                if "ui-tabs-active" not in operation_period_tab.get_attribute("class"):
                    logger.info("Clicking Operation Period Details tab to extract release date...")
                    operation_period_tab.click()
                    time.sleep(2)
                
                # Look for the operation period details content
                period_details_div = driver.find_element(By.ID, "operation-period-details")
                
                # Find the table with period information
                table = period_details_div.find_element(By.TAG_NAME, "table")
                rows = table.find_elements(By.TAG_NAME, "tr")
                
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if cells and len(cells) > 0:
                        period_text = cells[0].text.strip()
                        # Look for pattern like "6/13/2025 - 7/14/2025"
                        period_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})', period_text)
                        if period_match:
                            end_date_str = period_match.group(2)
                            try:
                                end_date_obj = datetime.strptime(end_date_str, '%m/%d/%Y')
                                release_date = str(int(end_date_obj.strftime('%Y%m%d')))
                                logger.info(f"Found operation period end date: {end_date_str} -> {release_date}")
                                break
                            except ValueError as e:
                                logger.warning(f"Could not parse period end date '{end_date_str}': {e}")
                
            except Exception as e:
                logger.warning(f"Could not extract operation period end date from web: {e}")
        
        # Fallback: extract from URL or use current date
        if not release_date:
            fallback_date = str(int(datetime.now().strftime('%Y%m%d')))
            logger.warning(f"Could not find operation period end date, using current date: {fallback_date}")
            return fallback_date
        
        return release_date

    def scrape_current_schedule_table(self, url):
        """Scrape the current schedule table from FRBNY website"""
        
        logger.info("=== SCRAPE_CURRENT_SCHEDULE_TABLE CALLED ===")
        
        # First try direct CSV access
        logger.info("Attempting direct CSV fetch...")
        csv_data = self.fetch_csv_direct(url)
        if csv_data:
            logger.info(f"✅ Direct CSV fetch succeeded - got {len(csv_data)} operations")
            return csv_data
        
        logger.info("❌ Direct CSV fetch failed, falling back to browser scraping...")
        # Fallback to browser scraping using the proven working method
        browser_data = self.scrape_with_browser(url)
        if browser_data:
            logger.info(f"✅ Browser scraping succeeded - got {len(browser_data)} operations")
        else:
            logger.error("❌ Browser scraping also failed")
        return browser_data
    
    def fetch_csv_direct(self, base_url):
        """IMPROVED: Direct CSV fetch with better release date handling"""
        try:
            # Extract base URL
            base_site_url = base_url.split('/markets')[0]
            csv_url = urljoin(base_site_url, '/medialibrary/media/markets/treasury-securities-schedule/current-schedule.csv')
            
            logger.info(f"📊 Attempting to fetch CSV directly from: {csv_url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            response = requests.get(csv_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse CSV content
            csv_content = response.text
            logger.info("✅ Successfully fetched CSV, parsing content...")
            
            # Use pandas to parse CSV
            df = pd.read_csv(io.StringIO(csv_content))
            logger.info(f"📈 CSV parsed successfully. Shape: {df.shape}")
            logger.info(f"📋 Columns: {list(df.columns)}")
            
            # Process the data to match expected format
            processed_data = self.process_csv_data(df)
            
            # CRITICAL: Extract release date using improved method with 30s wait
            if processed_data:
                logger.info("🔍 EXTRACTING RELEASE DATE FOR CSV DATA WITH FIXED METHOD...")
                
                # Use the improved extraction method with extended wait time
                release_date = self.extract_release_date_from_web_simple(base_url)
                
                if not release_date:
                    # Try BeautifulSoup as backup
                    logger.info("🔄 Trying BeautifulSoup as backup...")
                    release_date = self.extract_release_date_with_beautifulsoup(base_url)
                
                if release_date and self._is_reasonable_release_date(release_date):
                    logger.info(f"✅ Applying extracted release date {release_date} to {len(processed_data)} operations")
                    for operation in processed_data:
                        operation['release_date'] = int(release_date)
                else:
                    logger.warning("⚠️  Could not extract valid release date, using current date as fallback")
                    current_date = str(int(datetime.now().strftime('%Y%m%d')))
                    logger.warning(f"🔄 Using fallback date: {current_date}")
                    for operation in processed_data:
                        operation['release_date'] = int(current_date)
            
            if processed_data:
                self.data = processed_data
                self.source_type = 'web'
                return processed_data
            
        except Exception as e:
            logger.warning(f"❌ Direct CSV fetch failed: {str(e)}")
            
        return None
    
    def process_csv_data(self, df):
        """Process CSV data to match expected format"""
        try:
            processed_data = []
            
            # Expected columns from the website
            expected_columns = [
                'Operation Date', 'Operation Time (ET)', 'Settlement Date',
                'Operation Type', 'Security Type and Maturity', 'Maturity Range',
                'Maximum Operation Size'
            ]
            
            # Map CSV columns to expected format
            column_mapping = {}
            for col in df.columns:
                col_clean = col.strip()
                for expected in expected_columns:
                    if col_clean.lower().replace(' ', '') == expected.lower().replace(' ', ''):
                        column_mapping[col] = expected
                        break
                    elif expected.lower().replace(' ', '').replace('(', '').replace(')', '') in col_clean.lower().replace(' ', '').replace('(', '').replace(')', ''):
                        column_mapping[col] = expected
                        break
            
            logger.info(f"Column mapping: {column_mapping}")
            
            for _, row in df.iterrows():
                operation = {
                    'OPERATION DATE': '',
                    'OPERATION TIME (ET)': '',
                    'SETTLEMENT DATE': '',
                    'OPERATION TYPE': '',
                    'SECURITY TYPE AND MATURITY': '',
                    'MATURITY RANGE': '',
                    'MAXIMUM OPERATION CURRENCY': '',
                    'MAXIMUM OPERATION SIZE': '',
                    'MAXIMUM OPERATION MULTIPLIER': '',
                    'release_date': ''  # Will be set by apply_release_date_to_operations
                }
                
                # Map data from CSV to operation format
                for csv_col, mapped_col in column_mapping.items():
                    value = str(row[csv_col]).strip() if pd.notna(row[csv_col]) else ''
                    
                    if mapped_col == 'Maximum Operation Size':
                        # Parse the maximum operation size
                        currency, size, multiplier = self.parse_maximum_operation_size(value)
                        operation['MAXIMUM OPERATION CURRENCY'] = currency
                        operation['MAXIMUM OPERATION SIZE'] = size
                        operation['MAXIMUM OPERATION MULTIPLIER'] = multiplier
                    else:
                        # Map to uppercase with spaces replaced by underscores for consistency
                        operation_key = mapped_col.upper().replace(' ', ' ')
                        if operation_key in operation:
                            operation[operation_key] = value
                
                # Only add if we have essential data
                if operation['OPERATION DATE'] and operation['OPERATION TYPE']:
                    processed_data.append(operation)
                    logger.info(f"Processed operation: {operation['OPERATION DATE']} | {operation['OPERATION TYPE']}")
            
            # Note: Release date will be applied later when we have access to the driver
            logger.info(f"Successfully processed {len(processed_data)} operations from CSV")
            return processed_data
            
        except Exception as e:
            logger.error(f"Error processing CSV data: {str(e)}")
            return None

    def scrape_with_browser(self, url):
        """Proven browser scraping method based on working standalone code"""
        driver = self.setup_driver()
        
        if not driver:
            logger.error("Could not setup web driver")
            return None
        
        # Store the release date extracted from Operation Period Details
        extracted_release_date = None
        
        try:
            logger.info("Loading the webpage...")
            driver.get(url)
            
            # Wait for the page to load
            wait = WebDriverWait(driver, 30)
            
            # FIRST: Extract the operation period end date from the default Operation Period Details tab
            try:
                logger.info("Extracting release date from Operation Period Details tab...")
                
                # WAIT FOR MONTHLY DETAILS CONTENT TO LOAD
                logger.info("Waiting for monthly details content to load...")
                
                # Wait for the monthly-details div to be present (this is the correct ID!)
                wait.until(EC.presence_of_element_located((By.ID, "monthly-details")))
                logger.info("Monthly details div found, waiting for content...")
                
                # Wait additional time for AJAX/JavaScript to populate the content
                time.sleep(3)
                
                # Wait for actual content with date pattern to appear
                def content_has_date_pattern(driver):
                    try:
                        element = driver.find_element(By.ID, "monthly-details")
                        text = element.text.strip()
                        has_pattern = bool(re.search(r'\d{1,2}/\d{1,2}/\d{4}\s*-\s*\d{1,2}/\d{1,2}/\d{4}', text))
                        logger.debug(f"Checking for date pattern in monthly-details: Found: {has_pattern}")
                        return has_pattern
                    except:
                        return False
                
                # Wait up to 15 seconds for content with date pattern to appear
                logger.info("Waiting for date pattern to appear in monthly-details...")
                try:
                    WebDriverWait(driver, 15).until(content_has_date_pattern)
                    logger.info("Date pattern detected in monthly-details!")
                except Exception as e:
                    logger.warning(f"Timeout waiting for date pattern: {e}")
                    logger.info("Proceeding anyway to check what content is available...")
                
                # NOW extract the release date from the correct element
                extracted_release_date = None
                
                try:
                    element = driver.find_element(By.ID, "monthly-details")
                    element_text = element.text.strip()
                    logger.info(f"Found monthly-details element with text: '{element_text[:200]}...'")
                    
                    # Look for the FIRST date pattern (should be the current period)
                    period_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})', element_text)
                    if period_match:
                        start_date_str = period_match.group(1)
                        end_date_str = period_match.group(2)
                        
                        # Use the end date as release date
                        end_date_obj = datetime.strptime(end_date_str, '%m/%d/%Y')
                        extracted_release_date = str(int(end_date_obj.strftime('%Y%m%d')))
                        
                        logger.info(f"SUCCESS! Found operation period: {start_date_str} - {end_date_str}")
                        logger.info(f"Using end date as release date: {end_date_str} -> {extracted_release_date}")
                    else:
                        logger.warning("No date pattern found in monthly-details element")
                        
                except Exception as e:
                    logger.error(f"Error extracting from monthly-details: {e}")
                
                # Fallback: try ui-tabs-panel class if monthly-details fails
                if not extracted_release_date:
                    logger.info("Fallback: trying ui-tabs-panel class...")
                    try:
                        panels = driver.find_elements(By.CLASS_NAME, "ui-tabs-panel")
                        logger.info(f"Found {len(panels)} ui-tabs-panel elements")
                        
                        for i, panel in enumerate(panels):
                            panel_text = panel.text.strip()
                            if panel_text and len(panel_text) > 50:  # Only check substantial content
                                logger.info(f"Checking panel {i+1} with text: '{panel_text[:100]}...'")
                                
                                period_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})', panel_text)
                                if period_match:
                                    start_date_str = period_match.group(1)
                                    end_date_str = period_match.group(2)
                                    
                                    end_date_obj = datetime.strptime(end_date_str, '%m/%d/%Y')
                                    extracted_release_date = str(int(end_date_obj.strftime('%Y%m%d')))
                                    
                                    logger.info(f"SUCCESS! Found period in panel {i+1}: {start_date_str} - {end_date_str}")
                                    logger.info(f"Using end date as release date: {end_date_str} -> {extracted_release_date}")
                                    break
                                    
                    except Exception as e:
                        logger.error(f"Error with fallback method: {e}")
                
                if not extracted_release_date:
                    logger.error("FAILED: Could not extract release date from any method")
                else:
                    logger.info(f"FINAL RESULT: extracted_release_date = {extracted_release_date}")
                    
            except Exception as e:
                logger.error(f"CRITICAL ERROR in release date extraction: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
            
            # SECOND: Now switch to Current Schedule tab to get operations data
            try:
                current_schedule_tab = wait.until(
                    EC.element_to_be_clickable((By.ID, "tab2"))
                )
                logger.info("Found Current Schedule tab")
                
                # Check if the tab is not already active
                if "ui-tabs-active" not in current_schedule_tab.get_attribute("class"):
                    logger.info("Clicking Current Schedule tab...")
                    current_schedule_tab.click()
                    time.sleep(2)  # Wait for tab content to load
            except Exception as e:
                logger.warning(f"Could not click tab: {e}")
            
            # Wait for the current schedule table to be present and loaded
            logger.info("Waiting for table to load...")
            current_schedule_div = wait.until(
                EC.presence_of_element_located((By.ID, "current-schedule"))
            )
            
            # Wait a bit more for the AJAX/JavaScript to load the table content
            time.sleep(5)
            
            # Try to find the table within the current schedule div
            current_schedule_table = current_schedule_div.find_element(By.ID, "current-schedule-table")
            table = current_schedule_table.find_element(By.TAG_NAME, "table")
            
            # Extract table headers
            headers = []
            header_row = table.find_element(By.TAG_NAME, "thead").find_element(By.TAG_NAME, "tr")
            for th in header_row.find_elements(By.TAG_NAME, "th"):
                headers.append(th.text.strip())
            
            logger.info(f"Found headers: {headers}")
            
            # Extract table data
            data = []
            tbody = table.find_element(By.TAG_NAME, "tbody")
            rows = tbody.find_elements(By.TAG_NAME, "tr")
            
            logger.info(f"Found {len(rows)} data rows")
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                row_data = []
                
                for cell in cells:
                    # Get text and clean it up (remove extra whitespace, line breaks)
                    cell_text = cell.text.strip().replace('\n', ' ').replace('\r', ' ')
                    # Remove multiple spaces
                    cell_text = re.sub(r'\s+', ' ', cell_text)
                    row_data.append(cell_text)
                
                if row_data:  # Only add non-empty rows
                    data.append(row_data)
            
            # Create DataFrame with original columns
            df = pd.DataFrame(data, columns=headers)
            
            logger.debug(f"DataFrame columns before processing: {df.columns.tolist()}")
            logger.debug(f"DataFrame shape: {df.shape}")
            if not df.empty and 'Maximum Operation Size' in df.columns:
                logger.debug(f"Sample Maximum Operation Size values: {df['Maximum Operation Size'].head().tolist()}")
            
            # Process Maximum Operation Size column according to runbook requirements
            if 'Maximum Operation Size' in df.columns:
                logger.info("Processing Maximum Operation Size column...")
                
                # Parse Maximum Operation Size into separate columns
                max_op_data = df['Maximum Operation Size'].apply(self.parse_maximum_operation_size)
                
                # Debug: log the parsed data
                logger.debug(f"Parsed max operation data sample: {max_op_data.head().tolist()}")
                
                # Get the position of the original Maximum Operation Size column
                col_position = df.columns.get_loc('Maximum Operation Size')
                
                # Create new columns in the correct order
                df.insert(col_position, 'MAXIMUM OPERATION CURRENCY', [item[0] for item in max_op_data])
                df.insert(col_position + 1, 'MAXIMUM OPERATION SIZE_NEW', [item[1] for item in max_op_data])
                df.insert(col_position + 2, 'MAXIMUM OPERATION MULTIPLIER', [item[2] for item in max_op_data])
                
                # Remove the original Maximum Operation Size column and rename the new one
                df = df.drop('Maximum Operation Size', axis=1)
                df = df.rename(columns={'MAXIMUM OPERATION SIZE_NEW': 'MAXIMUM OPERATION SIZE'})
                
                logger.info(f"Added new columns. DataFrame columns now: {df.columns.tolist()}")
                logger.info("Reordered columns with MAXIMUM OPERATION CURRENCY before MAXIMUM OPERATION SIZE")
            else:
                logger.warning("'Maximum Operation Size' column not found in DataFrame!")
                logger.warning(f"Available columns: {df.columns.tolist()}")
            
            # Convert to expected format for compatibility
            processed_data = []
            for _, row in df.iterrows():
                operation = {
                    'OPERATION DATE': row.get('Operation Date', ''),
                    'OPERATION TIME (ET)': row.get('Operation Time (ET)', ''),
                    'SETTLEMENT DATE': row.get('Settlement Date', ''),
                    'OPERATION TYPE': row.get('Operation Type', ''),
                    'SECURITY TYPE AND MATURITY': row.get('Security Type and Maturity', ''),
                    'MATURITY RANGE': row.get('Maturity Range', ''),
                    'MAXIMUM OPERATION CURRENCY': row.get('MAXIMUM OPERATION CURRENCY', ''),
                    'MAXIMUM OPERATION SIZE': row.get('MAXIMUM OPERATION SIZE', ''),
                    'MAXIMUM OPERATION MULTIPLIER': row.get('MAXIMUM OPERATION MULTIPLIER', ''),
                    'release_date': ''  # Will be set below
                }
                processed_data.append(operation)
            
            logger.info(f"Successfully processed {len(processed_data)} operations")
            
            # Apply the extracted release date to all operations
            if processed_data and extracted_release_date:
                logger.info(f"Applying extracted release date {extracted_release_date} to all operations")
                for operation in processed_data:
                    operation['release_date'] = int(extracted_release_date)
            elif processed_data:
                # Fallback: use the old apply_release_date_to_operations method 
                logger.warning("No release date extracted from Operation Period Details, using fallback method")
                processed_data = self.apply_release_date_to_operations(processed_data, driver)
            
            if processed_data:
                self.data = processed_data
                self.source_type = 'web'
                return processed_data
            else:
                logger.error("No data processed from table")
                return None
            
        except Exception as e:
            logger.error(f"Browser scraping failed: {str(e)}")
            return None
            
        finally:
            if driver:
                driver.quit()

    def parse_pdf_fallback(self, pdf_path: str) -> List[Dict]:
        """Parse PDF file as fallback when web scraping fails"""
        
        if not os.path.exists(pdf_path):
            logger.error(f"PDF file not found: {pdf_path}")
            return None
            
        logger.info(f"Parsing PDF fallback: {pdf_path}")
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    logger.info(f"Processing PDF page {page_num + 1}")
                    
                    table = self.extract_table_from_pdf(page)
                    
                    if table:
                        operations = self.parse_pdf_table(table)
                        self.data.extend(operations)
                        logger.info(f"Found {len(operations)} operations on page {page_num + 1}")
                    else:
                        text_operations = self.parse_pdf_text_fallback(page.extract_text())
                        self.data.extend(text_operations)
            
            if self.data:
                self.source_type = 'pdf'
                # For PDF, we'll use the old logic (max date) since we don't have web driver access
                self.data = self.apply_release_date_to_operations(self.data, None)
            return self.data
            
        except Exception as e:
            logger.error(f"PDF parsing failed: {str(e)}")
            return None

    def extract_table_from_pdf(self, page) -> Optional[List[List]]:
        """Extract table from PDF page"""
        try:
            tables = page.extract_tables(table_settings={
                "vertical_strategy": "text",
                "horizontal_strategy": "text",
                "intersection_tolerance": 3,
                "text_tolerance": 3
            })
            
            if tables:
                for table in tables:
                    if table and len(table) > 5:
                        logger.debug(f"Selected table with {len(table)} rows")
                        return table
                return max(tables, key=len)
            
        except Exception as e:
            logger.warning(f"Table extraction failed: {str(e)}")
        
        return None

    def parse_pdf_table(self, table: List[List]) -> List[Dict]:
        """Parse PDF table data"""
        operations = []
        data_start = self.find_pdf_data_start(table)
        
        i = data_start
        while i < len(table):
            row1 = table[i] if i < len(table) else []
            row2 = table[i + 1] if i + 1 < len(table) else []
            
            if self.is_empty_row(row1) and self.is_empty_row(row2):
                i += 1
                continue
            
            operation = self.parse_pdf_operation(row1, row2)
            
            if operation:
                operations.append(operation)
                logger.info(f"Parsed PDF: {operation['OPERATION DATE']} | {operation['OPERATION TYPE']}")
                i += 2
            else:
                i += 1
        
        return operations

    def find_pdf_data_start(self, table: List[List]) -> int:
        """Find where data starts after headers in PDF"""
        for i, row in enumerate(table):
            if not row:
                continue
            
            row_text = ' '.join([str(cell) for cell in row if cell]).upper()
            
            if any(header in row_text for header in [
                'OPERATION DATE', 'OPERATION TIME', 'SETTLEMENT DATE',
                'OPERATION TYPE', 'SECURITY TYPE', 'MATURITY RANGE', 'MAXIMUM'
            ]):
                continue
            
            if re.search(r'\d{1,2}/\d{1,2}/\d{4}', row_text):
                return i
        
        return 0

    def is_empty_row(self, row: List) -> bool:
        """Check if row is empty"""
        if not row:
            return True
        return all(not cell or str(cell).strip() == '' for cell in row)

    def parse_pdf_operation(self, row1: List, row2: List) -> Optional[Dict]:
        """Parse operation from PDF row pair"""
        try:
            operation = {
                'OPERATION DATE': '',
                'OPERATION TIME (ET)': '',
                'SETTLEMENT DATE': '',
                'OPERATION TYPE': '',
                'SECURITY TYPE AND MATURITY': '',
                'MATURITY RANGE': '',
                'MAXIMUM OPERATION CURRENCY': '',
                'MAXIMUM OPERATION SIZE': '',
                'MAXIMUM OPERATION MULTIPLIER': '',
                'release_date': ''
            }
            
            # Parse columns
            self.parse_pdf_columns(row1, row2, operation)
            
            # Extract amount
            all_text = self.combine_row_text(row1, row2)
            self.extract_pdf_amount(all_text, operation)
            
            if self.is_valid_pdf_operation(operation):
                return operation
            
        except Exception as e:
            logger.warning(f"Error parsing PDF operation: {str(e)}")
        
        return None

    def combine_row_text(self, row1: List, row2: List) -> str:
        """Combine text from both rows"""
        all_cells = []
        if row1:
            all_cells.extend([str(cell).strip() if cell else '' for cell in row1])
        if row2:
            all_cells.extend([str(cell).strip() if cell else '' for cell in row2])
        return ' '.join(all_cells)

    def parse_pdf_columns(self, row1: List, row2: List, operation: dict):
        """Parse PDF data by column positions"""
        if not row1 or len(row1) < 3:
            return
        
        # Column parsing logic
        if row1[0] and re.search(r'\d{1,2}/\d{1,2}/\d{4}', str(row1[0])):
            operation['OPERATION DATE'] = str(row1[0]).strip()
        
        # Time parsing
        time_parts = []
        if len(row1) > 1 and row1[1]:
            time_parts.append(str(row1[1]).strip())
        if row2 and len(row2) > 1 and row2[1]:
            time_parts.append(str(row2[1]).strip())
        if time_parts:
            operation['OPERATION TIME (ET)'] = ' '.join(time_parts)
        
        # Settlement date
        if len(row1) > 2 and row1[2] and re.search(r'\d{1,2}/\d{1,2}/\d{4}', str(row1[2])):
            operation['SETTLEMENT DATE'] = str(row1[2]).strip()
        
        # Operation type
        if len(row1) > 3 and row1[3]:
            operation['OPERATION TYPE'] = str(row1[3]).strip()
        
        # Security type
        if len(row1) > 4 and row1[4]:
            operation['SECURITY TYPE AND MATURITY'] = str(row1[4]).strip()
        
        # Maturity range
        if len(row1) > 5 and row1[5]:
            operation['MATURITY RANGE'] = str(row1[5]).strip()

    def extract_pdf_amount(self, text: str, operation: dict):
        """Extract amount and multiplier from PDF"""
        logger.debug(f"Extracting amount from: {text}")
        
        pattern1 = re.search(r'\$(\d+(?:\.\d+)?)\s*(million|billion)', text, re.IGNORECASE)
        if pattern1:
            operation['MAXIMUM OPERATION SIZE'] = pattern1.group(1)
            operation['MAXIMUM OPERATION MULTIPLIER'] = pattern1.group(2).lower()
            return
        
        pattern2 = re.search(r'\b(\d+(?:\.\d+)?)\s*(million|billion)\b', text, re.IGNORECASE)
        if pattern2:
            operation['MAXIMUM OPERATION SIZE'] = pattern2.group(1)
            operation['MAXIMUM OPERATION MULTIPLIER'] = pattern2.group(2).lower()

    def parse_pdf_text_fallback(self, text: str) -> List[Dict]:
        """Fallback text parsing for PDF"""
        operations = []
        
        if not text:
            return operations
        
        lines = text.split('\n')
        current_operation = {}
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            date_match = re.search(r'^(\d{1,2}/\d{1,2}/\d{4})', line)
            if date_match:
                if current_operation and current_operation.get('OPERATION DATE'):
                    operations.append(self.complete_pdf_operation(current_operation))
                
                current_operation = {'OPERATION DATE': date_match.group(1)}
        
        if current_operation and current_operation.get('OPERATION DATE'):
            operations.append(self.complete_pdf_operation(current_operation))
        
        return operations

    def complete_pdf_operation(self, operation: dict) -> dict:
        """Complete PDF operation with all fields"""
        return {
            'OPERATION DATE': operation.get('OPERATION DATE', ''),
            'OPERATION TIME (ET)': operation.get('OPERATION TIME (ET)', ''),
            'SETTLEMENT DATE': operation.get('SETTLEMENT DATE', ''),
            'OPERATION TYPE': operation.get('OPERATION TYPE', ''),
            'SECURITY TYPE AND MATURITY': operation.get('SECURITY TYPE AND MATURITY', ''),
            'MATURITY RANGE': operation.get('MATURITY RANGE', ''),
            'MAXIMUM OPERATION CURRENCY': operation.get('MAXIMUM OPERATION CURRENCY', ''),
            'MAXIMUM OPERATION SIZE': operation.get('MAXIMUM OPERATION SIZE', ''),
            'MAXIMUM OPERATION MULTIPLIER': operation.get('MAXIMUM OPERATION MULTIPLIER', ''),
            'release_date': ''  # Will be set by apply_release_date_to_operations
        }

    def is_valid_pdf_operation(self, operation: dict) -> bool:
        """Check if PDF operation is valid"""
        has_date = bool(operation.get('OPERATION DATE'))
        has_type = bool(operation.get('OPERATION TYPE'))
        has_security = bool(operation.get('SECURITY TYPE AND MATURITY'))
        
        return has_date and (has_type or has_security)

    def standardize_output_format(self, operations: List[Dict]) -> List[Dict]:
        """Convert operations to pipeline-expected format (lowercase)"""
        standardized = []
        for op in operations:
            standardized_op = {
                'operation_date': op.get('OPERATION DATE', ''),
                'operation_time': op.get('OPERATION TIME (ET)', ''),
                'settlement_date': op.get('SETTLEMENT DATE', ''),
                'operation_type': op.get('OPERATION TYPE', ''),
                'security_type_and_maturity': op.get('SECURITY TYPE AND MATURITY', ''),
                'maturity_range': op.get('MATURITY RANGE', ''),
                'maximum_operation_currency': op.get('MAXIMUM OPERATION CURRENCY', ''),
                'maximum_operation_size': op.get('MAXIMUM OPERATION SIZE', ''),
                'maximum_operation_multiplier': op.get('MAXIMUM OPERATION MULTIPLIER', ''),
                'release_date': op.get('release_date', '')
            }
            standardized.append(standardized_op)
        return standardized

    def save_to_csv(self, filename="FEDAO_MOA_DATA.csv"):
        """Save data to CSV file"""
        if not self.data:
            logger.error("No data to save")
            return False
        
        # Standardize format before saving
        standardized_data = self.standardize_output_format(self.data)
        
        fieldnames = [
            'operation_date',
            'operation_time', 
            'settlement_date',
            'operation_type',
            'security_type_and_maturity',
            'maturity_range',
            'maximum_operation_currency',
            'maximum_operation_size',
            'maximum_operation_multiplier',
            'release_date'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for operation in standardized_data:
                row = {}
                for field in fieldnames:
                    value = operation.get(field, '')
                    # Clean up formatting - remove commas from string values
                    if isinstance(value, str):
                        value = value.replace(',', '').strip()
                    row[field] = value
                writer.writerow(row)
        
        logger.info(f"Data saved to {filename}")
        logger.info(f"Rows: {len(standardized_data)}")
        logger.info(f"Source: {self.source_type}")
        return True

    def run(self, url, pdf_path=None):
        """Main execution method"""
        logger.info("Starting Combined FRBNY Treasury Securities scraper...")
        
        # First, try web scraping
        logger.info("Attempting web scraping...")
        web_data = self.scrape_current_schedule_table(url)
        
        if web_data and len(web_data) > 0:
            logger.info(f"Successfully scraped {len(web_data)} operations from website")
            return True
        
        # If web scraping failed, try PDF fallback
        if pdf_path:
            logger.info("Web scraping failed/no data, trying PDF fallback...")
            pdf_data = self.parse_pdf_fallback(pdf_path)
            
            if pdf_data and len(pdf_data) > 0:
                logger.info(f"Successfully parsed {len(pdf_data)} operations from PDF")
                return True
            else:
                logger.error("PDF parsing also failed or returned no data")
                return False
        else:
            logger.error("No PDF provided for fallback")
            return False

    # PIPELINE COMPATIBILITY METHODS
    def parse_pdf(self, pdf_path: str) -> List[Dict]:
        """Compatibility method for pipeline integration"""
        pdf_data = self.parse_pdf_fallback(pdf_path)
        if pdf_data:
            # Convert to standardized format for pipeline
            return self.standardize_output_format(pdf_data)
        return []

    def to_csv(self, output_path: str) -> None:
        """Compatibility method for pipeline integration"""
        if self.data:
            # Use the save_to_csv method which handles standardization
            original_filename = output_path
            success = self.save_to_csv(original_filename)
            if not success:
                raise ValueError("Failed to save data to CSV")
        else:
            raise ValueError("No data to export")


# BACKWARD COMPATIBILITY WRAPPER
class FixedFRBNYParser:
    """Wrapper class for backward compatibility with existing pipeline"""
    
    def __init__(self):
        self.scraper = CombinedFRBNYScraper()
        self.data = []
    
    def parse_pdf(self, pdf_path: str) -> List[Dict]:
        """Legacy method signature for pipeline compatibility"""
        logger.info(f"FixedFRBNYParser: parsing {pdf_path}")
        result = self.scraper.parse_pdf(pdf_path)
        self.data = result
        return result
    
    def to_csv(self, output_path: str):
        """Legacy method signature for pipeline compatibility"""
        if self.data:
            fieldnames = [
                'operation_date',
                'operation_time', 
                'settlement_date',
                'operation_type',
                'security_type_and_maturity',
                'maturity_range',
                'maximum_operation_currency',
                'maximum_operation_size',
                'maximum_operation_multiplier',
                'release_date'
            ]
            
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for operation in self.data:
                    row = {}
                    for field in fieldnames:
                        value = operation.get(field, '')
                        # Clean up formatting
                        if isinstance(value, str):
                            value = value.replace(',', '').strip()
                        row[field] = value
                    writer.writerow(row)
            
            logger.info(f"Data exported to CSV: {output_path}")
        else:
            raise ValueError("No data to export")

    def print_summary(self) -> None:
        """Print summary for compatibility"""
        if not self.data:
            print("No data parsed.")
            return
        
        print("\nFRBNY Treasury Operations Summary")
        print("=" * 50)
        print("Total Operations: " + str(len(self.data)))
        print("Source: Web scraping with PDF fallback")
        
        complete_ops = sum(1 for op in self.data 
                          if op.get('operation_date') and op.get('operation_type') 
                          and op.get('security_type_and_maturity'))
        
        print("Complete Operations: " + str(complete_ops) + "/" + str(len(self.data)))
        print("=" * 50)

    def preview_data(self, num_rows: int = 5) -> None:
        """Preview data for compatibility"""
        if not self.data:
            print("No data to preview.")
            return
        
        print("\nData Preview (" + str(min(num_rows, len(self.data))) + " of " + str(len(self.data)) + " operations):")
        print("-" * 100)
        
        for i, operation in enumerate(self.data[:num_rows], 1):
            print("Operation " + str(i) + ":")
            print("  Date: " + operation.get('operation_date', 'N/A') + " | Time: " + operation.get('operation_time', 'N/A'))
            print("  Type: " + operation.get('operation_type', 'N/A'))
            print("  Security: " + operation.get('security_type_and_maturity', 'N/A'))
            print("  Amount: " + operation.get('maximum_operation_currency', '') + str(operation.get('maximum_operation_size', 'N/A')) + " " + operation.get('maximum_operation_multiplier', ''))
            print("  Release Date: " + str(operation.get('release_date', 'N/A')))
            print()


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='FIXED FRBNY Treasury Securities Scraper with Correct Release Date Extraction')
    parser.add_argument('--pdf', help='PDF file path for fallback')
    parser.add_argument('-o', '--output', help='Output CSV path', default='FEDAO_MOA_DATA.csv')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument('--url', help='FRBNY URL', 
                       default='https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details')
    parser.add_argument('--legacy-mode', action='store_true', help='Use legacy FixedFRBNYParser interface')
    parser.add_argument('--test-release-date', action='store_true', help='Test release date extraction only')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        if args.test_release_date:
            # Test only the release date extraction
            logger.info("🧪 Testing release date extraction only...")
            scraper = CombinedFRBNYScraper()
            
            # Test the improved extraction method
            release_date = scraper.extract_release_date_from_web_simple(args.url)
            
            if release_date:
                logger.info(f"✅ Release date extraction test PASSED: {release_date}")
                try:
                    date_obj = datetime.strptime(release_date, '%Y%m%d')
                    logger.info(f"📅 Parsed date: {date_obj.strftime('%B %d, %Y')}")
                    
                    # Expected: 20250714 (July 14, 2025) based on the HTML sample
                    if release_date == "20250714":
                        logger.info("🎯 PERFECT! Got expected release date 20250714 (July 14, 2025)")
                    else:
                        logger.warning(f"⚠️  Got {release_date}, expected 20250714")
                        
                except ValueError:
                    logger.error("❌ Invalid date format returned")
            else:
                logger.error("❌ Release date extraction test FAILED")
                
                # Try BeautifulSoup as backup test
                logger.info("🔄 Testing BeautifulSoup method...")
                release_date_bs = scraper.extract_release_date_with_beautifulsoup(args.url)
                
                if release_date_bs:
                    logger.info(f"✅ BeautifulSoup method PASSED: {release_date_bs}")
                else:
                    logger.error("❌ BeautifulSoup method also FAILED")
            
            return
        
        if args.legacy_mode:
            # Use legacy interface for testing compatibility
            parser_instance = FixedFRBNYParser()
            if args.pdf:
                operations = parser_instance.parse_pdf(args.pdf)
                if operations:
                    parser_instance.to_csv(args.output)
                    parser_instance.print_summary()
                    parser_instance.preview_data(3)
                else:
                    logger.error("No operations extracted")
            else:
                logger.error("PDF path required in legacy mode")
        else:
            # Use new interface with FIXED release date extraction
            scraper = CombinedFRBNYScraper()
            success = scraper.run(args.url, args.pdf)
            
            if success:
                if scraper.save_to_csv(args.output):
                    logger.info(f"Successfully completed! Data source: {scraper.source_type}")
                    
                    # Print summary
                    print(f"\n🎉 Scraping completed successfully!")
                    print(f"📊 Source: {scraper.source_type}")
                    print(f"📈 Operations found: {len(scraper.data)}")
                    print(f"💾 Saved to: {args.output}")
                    
                    if scraper.data:
                        # Show the release date that was applied
                        sample_release_date = scraper.data[0].get('release_date', 'N/A')
                        print(f"🗓️  Applied release date: {sample_release_date}")
                        
                        # Convert back to readable format
                        if sample_release_date and sample_release_date != 'N/A':
                            try:
                                readable_date = datetime.strptime(str(sample_release_date), '%Y%m%d').strftime('%B %d, %Y')
                                print(f"📅 Release date: {readable_date}")
                                
                                # Check if we got the expected date
                                if str(sample_release_date) == "20250714":
                                    print("✅ SUCCESS: Got expected release date 20250714 (July 14, 2025)")
                                elif str(sample_release_date) == "20250620":
                                    print("⚠️  WARNING: Got fallback current date instead of operation period end date")
                                else:
                                    print(f"ℹ️  INFO: Got release date {sample_release_date}")
                            except:
                                pass
                    
                    if scraper.data and args.verbose:
                        print(f"\n📋 Sample operation:")
                        sample = scraper.data[0]
                        for key, value in sample.items():
                            print(f"   {key}: {value}")
                else:
                    logger.error("Failed to save data")
                    sys.exit(1)
            else:
                logger.error("Failed to extract any data from web or PDF")
                sys.exit(1)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

# USAGE EXAMPLES:
#
# 1. Test release date extraction only:
#    python scraper.py --test-release-date
#
# 2. Run normal scraping with verbose output:
#    python scraper.py -v
#
# 3. Run with PDF fallback:
#    python scraper.py --pdf schedule.pdf
#
# 4. Test legacy mode:
#    python scraper.py --legacy-mode --pdf schedule.pdf
#
# Expected output:
# - Release date should be 20250714 (July 14, 2025) based on HTML: "6/13/2025 - 7/14/2025"
# - Should NOT be 20250620 (current date fallback)
#
# Key improvements:
# 1. Focuses on first <td> element only
# 2. 30+ second wait times for dynamic content
# 3. Multiple extraction strategies (regex + BeautifulSoup)
# 4. Enhanced validation and error handling
# 5. Better logging for debugging
# 6. Test mode for verification