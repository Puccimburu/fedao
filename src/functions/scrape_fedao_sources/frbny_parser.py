#!/usr/bin/env python3
"""
Complete FRBNY Scraper - Web First, PDF Fallback (Pipeline Compatible)
Integrates proven web scraping method with PDF fallback and pipeline compatibility
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
    """Complete scraper for FRBNY data - Web first, PDF fallback (Pipeline Compatible)"""
    
    def __init__(self):
        self.data = []
        self.source_type = None  # 'web' or 'pdf'
        
    def setup_driver(self):
        """Setup Chrome driver with appropriate options - proven working version"""
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
            # Remove headless mode to help with JavaScript execution during development
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

    def scrape_current_schedule_table(self, url):
        """Scrape the current schedule table from FRBNY website"""
        
        # First try direct CSV access
        csv_data = self.fetch_csv_direct(url)
        if csv_data:
            return csv_data
        
        # Fallback to browser scraping using the proven working method
        return self.scrape_with_browser(url)
    
    def fetch_csv_direct(self, base_url):
        """Directly fetch the CSV file that populates the table"""
        try:
            # Extract base URL
            base_site_url = base_url.split('/markets')[0]
            csv_url = urljoin(base_site_url, '/medialibrary/media/markets/treasury-securities-schedule/current-schedule.csv')
            
            logger.info(f"Attempting to fetch CSV directly from: {csv_url}")
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(csv_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse CSV content
            csv_content = response.text
            
            logger.info("Successfully fetched CSV, parsing content...")
            
            # Use pandas to parse CSV
            df = pd.read_csv(io.StringIO(csv_content))
            
            logger.info(f"CSV parsed successfully. Shape: {df.shape}")
            logger.info(f"Columns: {list(df.columns)}")
            
            # Process the data to match expected format
            processed_data = self.process_csv_data(df)
            
            if processed_data:
                self.data = processed_data
                self.source_type = 'web'
                return processed_data
            
        except Exception as e:
            logger.warning(f"Direct CSV fetch failed: {str(e)}")
            
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
                    'MAXIMUM OPERATION CURRENCY': '$',
                    'MAXIMUM OPERATION SIZE': '',
                    'MAXIMUM OPERATION MULTIPLIER': '',
                    'release_date': ''
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
                
                # Set release date
                current_date = datetime.now().strftime('%Y%m%d')
                operation['release_date'] = int(current_date)
                
                # Only add if we have essential data
                if operation['OPERATION DATE'] and operation['OPERATION TYPE']:
                    processed_data.append(operation)
                    logger.info(f"Processed operation: {operation['OPERATION DATE']} | {operation['OPERATION TYPE']}")
            
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
        
        try:
            logger.info("Loading the webpage...")
            driver.get(url)
            
            # Wait for the page to load
            wait = WebDriverWait(driver, 30)
            
            # Wait for the tabs to be clickable and click on "Current Schedule" tab if not active
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
                    'MAXIMUM OPERATION CURRENCY': row.get('MAXIMUM OPERATION CURRENCY', '$'),
                    'MAXIMUM OPERATION SIZE': row.get('MAXIMUM OPERATION SIZE', ''),
                    'MAXIMUM OPERATION MULTIPLIER': row.get('MAXIMUM OPERATION MULTIPLIER', ''),
                    'release_date': int(datetime.now().strftime('%Y%m%d'))
                }
                processed_data.append(operation)
            
            logger.info(f"Successfully processed {len(processed_data)} operations")
            
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
                'MAXIMUM OPERATION CURRENCY': '$',
                'MAXIMUM OPERATION SIZE': '',
                'MAXIMUM OPERATION MULTIPLIER': '',
                'release_date': ''
            }
            
            # Parse columns
            self.parse_pdf_columns(row1, row2, operation)
            
            # Extract amount
            all_text = self.combine_row_text(row1, row2)
            self.extract_pdf_amount(all_text, operation)
            
            # Set release date
            operation['release_date'] = self.calculate_release_date(operation)
            
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
            'MAXIMUM OPERATION CURRENCY': operation.get('MAXIMUM OPERATION CURRENCY', '$'),
            'MAXIMUM OPERATION SIZE': operation.get('MAXIMUM OPERATION SIZE', ''),
            'MAXIMUM OPERATION MULTIPLIER': operation.get('MAXIMUM OPERATION MULTIPLIER', ''),
            'release_date': self.calculate_release_date(operation)
        }

    def calculate_release_date(self, operation: dict) -> str:
        """Calculate release date"""
        if operation.get('OPERATION DATE'):
            try:
                date_obj = datetime.strptime(operation['OPERATION DATE'], '%m/%d/%Y')
                return str(int(date_obj.strftime('%Y%m%d')))
            except ValueError:
                pass
        
        if operation.get('SETTLEMENT DATE'):
            try:
                date_obj = datetime.strptime(operation['SETTLEMENT DATE'], '%m/%d/%Y')
                return str(int(date_obj.strftime('%Y%m%d')))
            except ValueError:
                pass
        
        return str(int(datetime.now().strftime('%Y%m%d')))

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
        
        # Convert to DataFrame if it's a list of dicts
        if isinstance(self.data, list):
            df = pd.DataFrame(self.data)
        else:
            df = self.data
            
        if df.empty:
            logger.error("No data to save")
            return False
        
        # Clean up formatting
        df = df.map(lambda x: str(x).strip() if pd.notna(x) else "")
        df = df.map(lambda x: x.replace(',', '') if isinstance(x, str) else x)
        
        df.to_csv(filename, index=False)
        logger.info(f"Data saved to {filename}")
        logger.info(f"Shape: {df.shape}")
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
            
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for operation in standardized_data:
                    row = {}
                    for field in fieldnames:
                        row[field] = operation.get(field, '')
                    writer.writerow(row)
            
            logger.info(f"Data exported to CSV: {output_path}")
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
                        row[field] = operation.get(field, '')
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
            print()


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Complete FRBNY Treasury Securities Scraper')
    parser.add_argument('--pdf', help='PDF file path for fallback')
    parser.add_argument('-o', '--output', help='Output CSV path', default='FEDAO_MOA_DATA.csv')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument('--url', help='FRBNY URL', 
                       default='https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details')
    parser.add_argument('--legacy-mode', action='store_true', help='Use legacy FixedFRBNYParser interface')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
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
            # Use new interface
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