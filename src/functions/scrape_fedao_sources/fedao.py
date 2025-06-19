#!/usr/bin/env python3
"""
FRBNY Automated PDF Scraper and Parser - INTEGRATED VERSION
Scrapes FRBNY website, downloads PDFs, and processes them with BOTH parsers
Supports both MOA (Mortgage Operations) and TOA (Treasury Operations) PDFs
"""

import os
import re
import sys
import csv
import time
import requests
import tempfile
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple
import argparse
import logging
from urllib.parse import urljoin, urlparse
from pathlib import Path

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException

# Import both parsers
try:
    from frbny_parser import CombinedFRBNYScraper  # TOA parser - Use actual class name
except ImportError:
    print("Warning: Could not import CombinedFRBNYScraper (TOA parser). TOA processing disabled.")
    CombinedFRBNYScraper = None

try:
    # Assuming the MOA parser class is named FEDAOParser from the first code
    # You may need to adjust the import based on your actual file structure
    from fedao_parser import FEDAOParser  # MOA parser
except ImportError:
    try:
        # Alternative import if the class is in a different module
        from main import FEDAOParser
    except ImportError:
        print("Warning: Could not import FEDAOParser (MOA parser). MOA processing disabled.")
        FEDAOParser = None

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IntegratedFRBNYScraper:
    """Enhanced scraper with dual parser support for both MOA and TOA PDFs"""
    
    def __init__(self, headless: bool = True, download_dir: str = None):
        self.base_url = "https://www.newyorkfed.org"
        self.toa_url = "https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details"
        self.moa_url = "https://www.newyorkfed.org/markets/ambs_operation_schedule#tabs-2"
        self.download_dir = download_dir or tempfile.mkdtemp()
        self.driver = None
        self.pdf_urls = set()
        self.moa_pdf_urls = set()  # MOA PDFs
        self.toa_pdf_urls = set()  # TOA PDFs
        self.moa_operations = []  # Mortgage operations
        self.toa_operations = []  # Treasury operations
        self.headless = headless
        
        # Ensure download directory exists
        Path(self.download_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Download directory: {self.download_dir}")
        
        # Check parser availability
        self.moa_parser_available = FEDAOParser is not None
        self.toa_parser_available = CombinedFRBNYScraper is not None
        
        if not self.moa_parser_available:
            logger.warning("MOA Parser not available - MOA PDFs will be skipped")
        if not self.toa_parser_available:
            logger.warning("TOA Parser not available - TOA PDFs will be skipped")
    
    def setup_driver(self) -> webdriver.Chrome:
        """Setup Chrome WebDriver with download preferences"""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Download preferences
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "plugins.always_open_pdf_externally": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
        except Exception as e:
            logger.error(f"Failed to initialize Chrome driver: {e}")
            logger.info("Please ensure ChromeDriver is installed and in PATH")
            raise
        
        return self.driver
    
    def scrape_moa_pdf_urls(self) -> Set[str]:
        """Scrape MOA PDF URLs from the MBS operation schedule page"""
        if not self.driver:
            self.setup_driver()
        
        logger.info("Starting MOA PDF URL collection...")
        moa_pdfs = set()
        
        try:
            logger.info(f"Navigating to MOA page: {self.moa_url}")
            self.driver.get(self.moa_url)
            
            # Wait for page to load
            time.sleep(3)
            
            # Look for the specific table structure in the "all-schedules" div
            try:
                # Wait for the table to be present
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "all-schedules"))
                )
                
                # Find the table within the all-schedules div
                schedules_div = self.driver.find_element(By.ID, "all-schedules")
                
                # Look for all PDF links within this table
                pdf_links = schedules_div.find_elements(By.CSS_SELECTOR, "a.pdf")
                
                for link in pdf_links:
                    href = link.get_attribute("href")
                    if href:
                        if href.startswith("/"):
                            full_url = self.base_url + href
                        else:
                            full_url = href
                        
                        moa_pdfs.add(full_url)
                        logger.debug(f"Found MOA PDF: {full_url}")
                
                # Also look for any links containing "AMBS-Schedule" in the href
                all_ambs_links = schedules_div.find_elements(By.CSS_SELECTOR, "a[href*='AMBS-Schedule']")
                for link in all_ambs_links:
                    href = link.get_attribute("href")
                    if href and href.endswith('.pdf'):
                        if href.startswith("/"):
                            full_url = self.base_url + href
                        else:
                            full_url = href
                        moa_pdfs.add(full_url)
                        logger.debug(f"Found AMBS PDF: {full_url}")
                
                # Additional fallback: look for any PDF links in the ambs directory
                all_links = schedules_div.find_elements(By.TAG_NAME, "a")
                for link in all_links:
                    href = link.get_attribute("href")
                    if href and ('/ambs/' in href or 'AMBS' in href) and href.endswith('.pdf'):
                        if href.startswith("/"):
                            full_url = self.base_url + href
                        else:
                            full_url = href
                        moa_pdfs.add(full_url)
                        logger.debug(f"Found AMBS directory PDF: {full_url}")
                
            except TimeoutException:
                logger.warning("Could not find all-schedules table, trying alternative selectors")
                
                # Fallback: look for any PDF links that might be MOA-related
                pdf_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='.pdf']")
                for link in pdf_links:
                    href = link.get_attribute("href")
                    if href and any(keyword in href.lower() for keyword in ['ambs', 'mbs', 'mortgage']):
                        if href.startswith("/"):
                            full_url = self.base_url + href
                        else:
                            full_url = href
                        moa_pdfs.add(full_url)
                        logger.debug(f"Found MOA-related PDF: {full_url}")
            
            logger.info(f"Found {len(moa_pdfs)} PDFs from MOA page")
            self.moa_pdf_urls = moa_pdfs
            return moa_pdfs
            
        except Exception as e:
            logger.error(f"Error scraping MOA PDFs: {e}")
            return set()
    
    def scrape_toa_pdf_urls(self) -> Set[str]:
        """Scrape TOA PDF URLs from Treasury Securities page (original functionality)"""
        if not self.driver:
            self.setup_driver()
        
        logger.info("Starting TOA PDF URL collection from all pages...")
        toa_pdfs = set()
        
        try:
            logger.info(f"Navigating to TOA page: {self.toa_url}")
            self.driver.get(self.toa_url)
            
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "pagination-table"))
            )
            
            current_page = 1
            max_pages = self.get_max_pages()
            
            logger.info(f"Found {max_pages} TOA pages to process")
            
            while current_page <= max_pages:
                logger.info(f"Processing TOA page {current_page}/{max_pages}")
                
                page_pdfs = self.extract_pdf_urls_from_page()
                toa_pdfs.update(page_pdfs)
                
                logger.info(f"Found {len(page_pdfs)} PDFs on TOA page {current_page}")
                
                if current_page < max_pages:
                    if not self.go_to_next_page():
                        logger.warning(f"Could not navigate to TOA page {current_page + 1}")
                        break
                
                current_page += 1
                time.sleep(2)
            
            logger.info(f"Found {len(toa_pdfs)} PDFs from TOA pages")
            self.toa_pdf_urls = toa_pdfs
            return toa_pdfs
            
        except Exception as e:
            logger.error(f"Error scraping TOA PDFs: {e}")
            return set()
    
    def scrape_all_pdf_urls(self) -> Set[str]:
        """Scrape PDFs from both MOA and TOA sites"""
        logger.info("Starting comprehensive PDF URL collection from both sites...")
        
        # Scrape MOA PDFs
        moa_pdfs = self.scrape_moa_pdf_urls()
        
        # Scrape TOA PDFs  
        toa_pdfs = self.scrape_toa_pdf_urls()
        
        # Combine all PDFs
        all_pdfs = moa_pdfs.union(toa_pdfs)
        self.pdf_urls = all_pdfs
        
        logger.info(f"Total PDFs collected - MOA: {len(moa_pdfs)}, TOA: {len(toa_pdfs)}, Total: {len(all_pdfs)}")
        return all_pdfs
    
    def get_max_pages(self) -> int:
        """Get the maximum number of pages from pagination"""
        try:
            pagination_elements = self.driver.find_elements(By.CSS_SELECTOR, ".paginationjs-page")
            
            if not pagination_elements:
                logger.info("No pagination found, assuming single page")
                return 1
            
            max_page = 1
            for element in pagination_elements:
                try:
                    page_num = int(element.get_attribute("data-num"))
                    max_page = max(max_page, page_num)
                except (ValueError, TypeError):
                    continue
            
            last_page_elements = self.driver.find_elements(By.CSS_SELECTOR, ".paginationjs-last")
            for element in last_page_elements:
                try:
                    page_num = int(element.get_attribute("data-num"))
                    max_page = max(max_page, page_num)
                except (ValueError, TypeError):
                    continue
            
            return max_page
            
        except Exception as e:
            logger.warning(f"Could not determine max pages: {e}")
            return 1
    
    def extract_pdf_urls_from_page(self) -> Set[str]:
        """Extract PDF URLs from the current page"""
        pdf_urls = set()
        
        try:
            pdf_links = self.driver.find_elements(By.CSS_SELECTOR, "a.pdf")
            
            for link in pdf_links:
                href = link.get_attribute("href")
                if href:
                    if href.startswith("/"):
                        full_url = self.base_url + href
                    else:
                        full_url = href
                    
                    pdf_urls.add(full_url)
                    logger.debug(f"Found PDF: {full_url}")
            
            all_links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='.pdf']")
            for link in all_links:
                href = link.get_attribute("href")
                if href and href.endswith('.pdf'):
                    if href.startswith("/"):
                        full_url = self.base_url + href
                    else:
                        full_url = href
                    pdf_urls.add(full_url)
            
        except Exception as e:
            logger.error(f"Error extracting PDF URLs from page: {e}")
        
        return pdf_urls
    
    def go_to_next_page(self) -> bool:
        """Navigate to the next page"""
        try:
            next_button = self.driver.find_element(By.CSS_SELECTOR, ".paginationjs-next:not(.disabled)")
            
            if next_button:
                self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                time.sleep(1)
                
                try:
                    next_button.click()
                except ElementClickInterceptedException:
                    self.driver.execute_script("arguments[0].click();", next_button)
                
                time.sleep(3)
                
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "pagination-table"))
                )
                
                return True
                
        except (NoSuchElementException, TimeoutException):
            logger.info("No more pages available or next button not found")
            return False
        except Exception as e:
            logger.error(f"Error navigating to next page: {e}")
            return False
        
        return False
    
    def download_pdf(self, url: str) -> Optional[str]:
        """Download a single PDF file and track its source"""
        try:
            logger.info(f"Downloading: {url}")
            
            filename = os.path.basename(urlparse(url).path)
            if not filename.endswith('.pdf'):
                filename += '.pdf'
            
            # Add source hint to filename to help with classification
            if 'ambs_operation_schedule' in url:
                filename = f"MOA_{filename}"
            elif 'treasury-securities' in url:
                filename = f"TOA_{filename}"
            
            filepath = os.path.join(self.download_dir, filename)
            
            if os.path.exists(filepath):
                logger.info(f"File already exists: {filename}")
                return filepath
            
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded: {filename} ({os.path.getsize(filepath)} bytes)")
            return filepath
            
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return None
    
    def download_all_pdfs(self) -> List[str]:
        """Download all collected PDFs"""
        if not self.pdf_urls:
            logger.warning("No PDF URLs to download")
            return []
        
        downloaded_files = []
        
        logger.info(f"Starting download of {len(self.pdf_urls)} PDFs...")
        
        for i, url in enumerate(sorted(self.pdf_urls), 1):
            logger.info(f"Downloading PDF {i}/{len(self.pdf_urls)}")
            
            filepath = self.download_pdf(url)
            if filepath:
                downloaded_files.append(filepath)
            
            time.sleep(1)
        
        logger.info(f"Successfully downloaded {len(downloaded_files)} PDFs")
        return downloaded_files
    
    def detect_pdf_type(self, pdf_path: str, source_hint: str = None) -> str:
        """Detect whether PDF is MOA or TOA type"""
        try:
            # If we know the source URL, use that as a strong hint
            if source_hint:
                if 'ambs_operation_schedule' in source_hint or 'mbs' in source_hint.lower():
                    logger.debug(f"PDF from MOA site: {os.path.basename(pdf_path)}")
                    return "MOA"
                elif 'treasury-securities' in source_hint:
                    logger.debug(f"PDF from TOA site: {os.path.basename(pdf_path)}")
                    return "TOA"
            
            # Fallback to content analysis
            import PyPDF2
            
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                # Check first few pages
                for page_num in range(min(3, len(pdf_reader.pages))):
                    text += pdf_reader.pages[page_num].extract_text() + "\n"
            
            text_lower = text.lower()
            
            # MOA indicators
            moa_indicators = [
                "small value operations",
                "tba purchase",
                "ginnie mae",
                "mbs",
                "mortgage-backed securities",
                "fnci",
                "g2sf",
                "fedtrade",
                "agency mbs"
            ]
            
            # TOA indicators  
            toa_indicators = [
                "treasury securities",
                "treasury bills",
                "treasury coupons", 
                "tips",
                "treasury frns",
                "nominal",
                "strips",
                "operation date",
                "settlement date",
                "maturity range"
            ]
            
            moa_score = sum(1 for indicator in moa_indicators if indicator in text_lower)
            toa_score = sum(1 for indicator in toa_indicators if indicator in text_lower)
            
            logger.debug(f"PDF type detection for {os.path.basename(pdf_path)}: MOA={moa_score}, TOA={toa_score}")
            
            if moa_score > toa_score:
                return "MOA"
            elif toa_score > moa_score:
                return "TOA"
            else:
                # Default based on filename or URL patterns
                filename = os.path.basename(pdf_path).lower()
                if any(keyword in filename for keyword in ['mbs', 'mortgage', 'ambs', 'small']):
                    logger.info(f"Defaulting to MOA based on filename: {filename}")
                    return "MOA"
                else:
                    logger.warning(f"Could not clearly determine PDF type for {os.path.basename(pdf_path)}, defaulting to TOA")
                    return "TOA"
            
        except Exception as e:
            logger.error(f"Error detecting PDF type for {pdf_path}: {e}")
            return "TOA"  # Default to TOA
    
    def process_pdfs_with_dual_parsers(self, pdf_files: List[str]) -> Tuple[List[Dict], List[Dict]]:
        """Process PDFs with appropriate parsers based on type detection"""
        moa_operations = []
        toa_operations = []
        
        logger.info(f"Processing {len(pdf_files)} PDF files with dual parser system...")
        
        for i, pdf_file in enumerate(pdf_files, 1):
            logger.info(f"Processing PDF {i}/{len(pdf_files)}: {os.path.basename(pdf_file)}")
            
            try:
                # Get source hint from filename
                filename = os.path.basename(pdf_file)
                source_hint = None
                if filename.startswith('MOA_'):
                    source_hint = 'ambs_operation_schedule'
                elif filename.startswith('TOA_'):
                    source_hint = 'treasury-securities'
                
                # Detect PDF type with source hint
                pdf_type = self.detect_pdf_type(pdf_file, source_hint)
                logger.info(f"Detected type: {pdf_type}")
                
                if pdf_type == "MOA" and self.moa_parser_available:
                    # Process with MOA parser
                    logger.info("Processing with MOA parser (FEDAOParser)")
                    parser = FEDAOParser()
                    operations = parser.parse_pdf(pdf_file)
                    
                    if operations:
                        moa_operations.extend(operations)
                        logger.info(f"Extracted {len(operations)} MOA operations")
                    else:
                        logger.warning(f"No MOA operations found in {os.path.basename(pdf_file)}")
                
                elif pdf_type == "TOA" and self.toa_parser_available:
                    # Process with TOA parser
                    logger.info("Processing with TOA parser (CombinedFRBNYScraper)")
                    parser = CombinedFRBNYScraper()
                    operations = parser.parse_pdf(pdf_file)
                    
                    if operations:
                        toa_operations.extend(operations)
                        logger.info(f"Extracted {len(operations)} TOA operations")
                    else:
                        logger.warning(f"No TOA operations found in {os.path.basename(pdf_file)}")
                
                else:
                    if pdf_type == "MOA" and not self.moa_parser_available:
                        logger.warning(f"MOA PDF detected but MOA parser not available: {os.path.basename(pdf_file)}")
                    elif pdf_type == "TOA" and not self.toa_parser_available:
                        logger.warning(f"TOA PDF detected but TOA parser not available: {os.path.basename(pdf_file)}")
                
            except Exception as e:
                logger.error(f"Error processing {pdf_file}: {e}")
                continue
        
        logger.info(f"Total operations extracted - MOA: {len(moa_operations)}, TOA: {len(toa_operations)}")
        return moa_operations, toa_operations
    
    def save_moa_operations_to_csv(self, operations: List[Dict], output_file: str) -> None:
        """Save MOA operations to CSV (FEDAO format)"""
        if not operations:
            logger.warning("No MOA operations to save")
            return
        
        # MOA fieldnames based on FEDAO format
        fieldnames = [
            'OperationDate',
            'OperationTime', 
            'Operation Type',
            'Settlement Date',
            'Securities Included (CUSP)',
            'Security Maximums (Millions)',
            'OperationMaximum',
            'Source_Date'
        ]
        
        sorted_operations = sorted(operations, key=lambda x: (
            x.get('Source_Date', ''), 
            x.get('OperationDate', ''), 
            x.get('OperationTime', '')
        ))
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for operation in sorted_operations:
                row = {}
                for field in fieldnames:
                    row[field] = operation.get(field, '')
                writer.writerow(row)
        
        logger.info(f"Saved {len(operations)} MOA operations to {output_file}")
    
    def save_toa_operations_to_csv(self, operations: List[Dict], output_file: str) -> None:
        """Save TOA operations to CSV (FixedFRBNY format)"""
        if not operations:
            logger.warning("No TOA operations to save")
            return
        
        # TOA fieldnames based on CombinedFRBNYScraper format
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
        
        sorted_operations = sorted(operations, key=lambda x: (
            x.get('operation_date', ''), 
            x.get('operation_time', '')
        ))
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for operation in sorted_operations:
                row = {}
                for field in fieldnames:
                    row[field] = operation.get(field, '')
                writer.writerow(row)
        
        logger.info(f"Saved {len(operations)} TOA operations to {output_file}")
    

    
    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver closed")
    
    def run_full_process(self, output_base: str = "FRBNY_Operations") -> None:
        """Run the complete scraping and processing workflow"""
        try:
            logger.info("Starting integrated FRBNY automated scraping and processing...")
            
            # Step 1: Setup WebDriver
            self.setup_driver()
            
            # Step 2: Scrape all PDF URLs
            self.scrape_all_pdf_urls()
            
            if not self.pdf_urls:
                logger.error("No PDF URLs found")
                return
            
            # Step 3: Download all PDFs
            pdf_files = self.download_all_pdfs()
            
            if not pdf_files:
                logger.error("No PDFs downloaded successfully")
                return
            
            # Step 4: Process PDFs with dual parsers
            moa_operations, toa_operations = self.process_pdfs_with_dual_parsers(pdf_files)
            
            if not moa_operations and not toa_operations:
                logger.error("No operations extracted from any PDFs")
                return
            
            # Step 5: Save outputs
            if moa_operations:
                moa_output = f"{output_base}_MOA.csv"
                self.save_moa_operations_to_csv(moa_operations, moa_output)
            
            if toa_operations:
                toa_output = f"{output_base}_TOA.csv" 
                self.save_toa_operations_to_csv(toa_operations, toa_output)
            
            # Step 6: Print summary
            self.print_summary(moa_operations, toa_operations, output_base)
            
        except Exception as e:
            logger.error(f"Error in full process: {e}")
            raise
        finally:
            self.cleanup()
    
    def print_summary(self, moa_operations: List[Dict], toa_operations: List[Dict], output_base: str) -> None:
        """Print comprehensive processing summary"""
        print("\n" + "="*80)
        print("INTEGRATED FRBNY AUTOMATED PROCESSING SUMMARY")
        print("="*80)
        print(f"Total PDFs discovered: {len(self.pdf_urls)}")
        print(f"MOA operations extracted: {len(moa_operations)}")
        print(f"TOA operations extracted: {len(toa_operations)}")
        print(f"Total operations: {len(moa_operations) + len(toa_operations)}")
        print(f"Download directory: {self.download_dir}")
        
        print(f"\nOutput files generated:")
        if moa_operations:
            print(f"  - {output_base}_MOA.csv ({len(moa_operations)} operations)")
        if toa_operations:
            print(f"  - {output_base}_TOA.csv ({len(toa_operations)} operations)")
        
        if moa_operations:
            print(f"\nMOA Operations by type:")
            moa_types = {}
            for op in moa_operations:
                op_type = op.get('Operation Type', 'Unknown')
                moa_types[op_type] = moa_types.get(op_type, 0) + 1
            for op_type, count in sorted(moa_types.items()):
                print(f"  {op_type}: {count}")
        
        if toa_operations:
            print(f"\nTOA Operations by type:")
            toa_types = {}
            for op in toa_operations:
                op_type = op.get('operation_type', 'Unknown')
                toa_types[op_type] = toa_types.get(op_type, 0) + 1
            for op_type, count in sorted(toa_types.items()):
                print(f"  {op_type}: {count}")
        
        # Date range
        all_dates = []
        for op in moa_operations:
            if op.get('OperationDate'):
                all_dates.append(op['OperationDate'])
        for op in toa_operations:
            if op.get('operation_date'):
                all_dates.append(op['operation_date'])
        
        if all_dates:
            all_dates.sort()
            print(f"\nOperation date range: {all_dates[0]} to {all_dates[-1]}")
        
        print("="*80)
        print("✅ COMPLETE AUTOMATION ACHIEVED - Both MOA and TOA processing!")


def main():
    """Main function with enhanced options"""
    parser = argparse.ArgumentParser(description='Integrated FRBNY Automated PDF Scraper and Parser')
    parser.add_argument('-o', '--output', default='FRBNY_Operations', 
                        help='Output base filename (default: FRBNY_Operations)')
    parser.add_argument('-d', '--download-dir', 
                        help='Directory to save PDFs (default: temp directory)')
    parser.add_argument('--headless', action='store_true', default=True,
                        help='Run browser in headless mode (default: True)')
    parser.add_argument('--show-browser', action='store_true',
                        help='Show browser window (disable headless mode)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose logging')
    parser.add_argument('--keep-pdfs', action='store_true',
                        help='Keep downloaded PDFs after processing')
    parser.add_argument('--moa-only', action='store_true',
                        help='Process only MOA PDFs')
    parser.add_argument('--toa-only', action='store_true',
                        help='Process only TOA PDFs')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Handle headless mode
    headless_mode = args.headless and not args.show_browser
    
    try:
        # Initialize integrated scraper
        scraper = IntegratedFRBNYScraper(
            headless=headless_mode,
            download_dir=args.download_dir
        )
        
        # Check if both parsers are available
        if args.moa_only and not scraper.moa_parser_available:
            print("Error: MOA-only mode requested but MOA parser not available")
            sys.exit(1)
        
        if args.toa_only and not scraper.toa_parser_available:
            print("Error: TOA-only mode requested but TOA parser not available")
            sys.exit(1)
        
        if not scraper.moa_parser_available and not scraper.toa_parser_available:
            print("Error: Neither MOA nor TOA parsers are available")
            sys.exit(1)
        
        # Run the complete integrated process
        scraper.run_full_process(args.output)
        
        # Clean up downloaded PDFs if not keeping them
        if not args.keep_pdfs and scraper.download_dir.startswith('/tmp'):
            import shutil
            shutil.rmtree(scraper.download_dir, ignore_errors=True)
            logger.info("Cleaned up temporary PDF files")
        
        print(f"\n🎉 Integrated processing completed successfully!")
        print(f"📊 Results saved with base name: {args.output}")
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()