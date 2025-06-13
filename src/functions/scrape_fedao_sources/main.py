import json
import base64
import logging
import os
import requests
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, timezone
import re
from urllib.parse import urljoin, urlparse
import functions_framework
from google.cloud import storage, pubsub_v1
from bs4 import BeautifulSoup
import tempfile
import traceback
from typing import List, Dict, Any, Optional
from google.cloud import aiplatform
from vertexai.generative_models import GenerativeModel, Part
import pdfplumber
from pathlib import Path
import csv

# Initialize logger at the module level
logger = logging.getLogger(__name__)

# INLINE UTILITY FUNCTIONS
def setup_logging(customer_id: str, project_name: str):
    """Setup minimal logging configuration."""
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(f"{customer_id}.{project_name}")

def load_customer_config(customer_id: str):
    """Load customer configuration."""
    return {
        "gcp_project_id": os.environ.get("GCP_PROJECT", "execo-simba")
    }

def load_dynamic_site_config(db, project_name: str, logger):
    """Load dynamic site configuration."""
    return {}

class DataValidationError(Exception):
    """Custom exception for data validation failures"""
    pass

class ExtractionValidator:
    """Simplified validation system for extracted data"""
    
    def __init__(self, logger):
        self.logger = logger
    
    def validate_data_structure(self, data: List[Dict], pdf_url: str) -> List[Dict]:
        """Validate data structure and content quality"""
        if not data:
            raise DataValidationError(f"Empty dataset for {pdf_url}")
        
        validation_issues = []
        
        if len(data) < 1:
            validation_issues.append("No data rows found")
        
        if not data[0]:
            validation_issues.append("No columns found in first row")
        else:
            columns = list(data[0].keys())
            
            if len(columns) < 2:
                validation_issues.append(f"Too few columns: {len(columns)}")
            
            empty_cols = [col for col in columns if not str(col).strip()]
            if empty_cols:
                validation_issues.append(f"Empty column names found: {len(empty_cols)}")
            
            total_cells = len(data) * len(columns)
            empty_cells = sum(1 for row in data for col, val in row.items() if not str(val).strip())
            
            if total_cells > 0:
                empty_percentage = empty_cells / total_cells
                if empty_percentage > 0.5:
                    validation_issues.append(f"High percentage of empty cells: {empty_percentage:.1%}")
        
        critical_issues = [issue for issue in validation_issues if any(keyword in issue.lower() 
                          for keyword in ['no data', 'no columns', 'too few columns'])]
        
        if critical_issues:
            raise DataValidationError(f"Critical validation issues for {pdf_url}: {'; '.join(critical_issues)}")
        
        if validation_issues:
            self.logger.warning(f"Data validation warnings for {pdf_url}: {'; '.join(validation_issues)}")
        
        return data

class EnhancedFRBNYParser:
    """Enhanced FRBNY Parser integrated into GCF - based on our working clean parser"""
    
    def __init__(self, logger):
        self.data = []
        self.pdf_path = None
        self.logger = logger
        
    def parse_pdf_content(self, pdf_content: bytes, pdf_url: str) -> List[Dict]:
        """Parse PDF content and extract FRBNY operations data"""
        self.pdf_path = pdf_url
        self.data = []
        
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file.write(pdf_content)
            temp_file.flush()
            
            try:
                with pdfplumber.open(temp_file.name) as pdf:
                    for page_num, page in enumerate(pdf.pages):
                        self.logger.info(f"Processing page {page_num + 1} of {pdf_url}")
                        
                        table = self.extract_table(page)
                        
                        if table:
                            operations = self.parse_table(table)
                            self.data.extend(operations)
                            self.logger.info(f"Found {len(operations)} operations on page {page_num + 1}")
                        else:
                            text_operations = self.parse_text_fallback(page.extract_text())
                            self.data.extend(text_operations)
                
                os.unlink(temp_file.name)
                return self.data
                
            except Exception as e:
                self.logger.error(f"Error parsing PDF {pdf_url}: {e}")
                os.unlink(temp_file.name)
                return []
    
    def extract_table(self, page) -> Optional[List[List]]:
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
                        self.logger.debug(f"Selected table with {len(table)} rows")
                        return table
                return max(tables, key=len)
            
        except Exception as e:
            self.logger.warning(f"Table extraction failed: {e}")
        
        return None
    
    def parse_table(self, table: List[List]) -> List[Dict]:
        """Parse table data"""
        operations = []
        data_start = self.find_data_start(table)
        
        i = data_start
        while i < len(table):
            row1 = table[i] if i < len(table) else []
            row2 = table[i + 1] if i + 1 < len(table) else []
            
            if self.is_empty_row(row1) and self.is_empty_row(row2):
                i += 1
                continue
            
            operation = self.parse_operation(row1, row2)
            
            if operation:
                operations.append(operation)
                self.logger.info(f"Parsed: {operation['operation_date']} | {operation['operation_type']}")
                i += 2
            else:
                i += 1
        
        return operations
    
    def find_data_start(self, table: List[List]) -> int:
        """Find where data starts after headers"""
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
    
    def parse_operation(self, row1: List, row2: List) -> Optional[Dict]:
        """Parse operation from row pair"""
        try:
            operation = {
                'operation_date': '',
                'operation_time': '',
                'settlement_date': '',
                'operation_type': '',
                'security_type_and_maturity': '',
                'maturity_range': '',
                'maximum_operation_currency': '$',
                'maximum_operation_size': '',
                'maximum_operation_multiplier': '',
                'release_date': ''
            }
            
            all_text = self.combine_row_text(row1, row2)
            
            self.parse_columns(row1, row2, operation)
            self.extract_amount(all_text, operation)
            self.enhance_operation(all_text, operation)
            
            operation['release_date'] = self.calculate_release_date(operation)
            
            if self.is_valid_operation(operation):
                return operation
            
        except Exception as e:
            self.logger.warning(f"Error parsing operation: {e}")
        
        return None
    
    def combine_row_text(self, row1: List, row2: List) -> str:
        """Combine text from both rows"""
        all_cells = []
        if row1:
            all_cells.extend([str(cell).strip() if cell else '' for cell in row1])
        if row2:
            all_cells.extend([str(cell).strip() if cell else '' for cell in row2])
        return ' '.join(all_cells)
    
    def parse_columns(self, row1: List, row2: List, operation: dict):
        """Parse data by column positions"""
        if not row1 or len(row1) < 7:
            return
        
        # Column 0: Operation Date
        if row1[0] and re.search(r'\d{1,2}/\d{1,2}/\d{4}', str(row1[0])):
            operation['operation_date'] = str(row1[0]).strip()
        
        # Column 1: Operation Time
        time_parts = []
        if row1[1]:
            time_parts.append(str(row1[1]).strip())
        if row2 and len(row2) > 1 and row2[1]:
            time_parts.append(str(row2[1]).strip())
        if time_parts:
            operation['operation_time'] = ' '.join(time_parts)
        
        # Column 2: Settlement Date
        if row1[2] and re.search(r'\d{1,2}/\d{1,2}/\d{4}', str(row1[2])):
            operation['settlement_date'] = str(row1[2]).strip()
        
        # Column 3: Operation Type
        type_parts = []
        if row1[3]:
            type_parts.append(str(row1[3]).strip())
        if row2 and len(row2) > 3 and row2[3]:
            type_parts.append(str(row2[3]).strip())
        if type_parts:
            operation['operation_type'] = ' '.join(type_parts)
        
        # Column 4: Security Type
        security_parts = []
        if row1[4]:
            security_parts.append(str(row1[4]).strip())
        if row2 and len(row2) > 4 and row2[4]:
            security_parts.append(str(row2[4]).strip())
        if security_parts:
            operation['security_type_and_maturity'] = ' '.join(security_parts)
        
        # Column 5: Maturity Range
        maturity_parts = []
        if row1[5]:
            maturity_parts.append(str(row1[5]).strip())
        if row2 and len(row2) > 5 and row2[5]:
            maturity_parts.append(str(row2[5]).strip())
        if maturity_parts:
            operation['maturity_range'] = ' '.join(maturity_parts)
    
    def extract_amount(self, text: str, operation: dict):
        """Extract amount and multiplier - ENHANCED VERSION"""
        self.logger.debug(f"Extracting amount from: {text}")
        
        if operation.get('maximum_operation_size') and operation.get('maximum_operation_multiplier'):
            return
        
        # Pattern 1: $80 million (most reliable)
        pattern1 = re.search(r'\$(\d+(?:\.\d+)?)\s*(million|billion)', text, re.IGNORECASE)
        if pattern1:
            operation['maximum_operation_size'] = pattern1.group(1)
            operation['maximum_operation_multiplier'] = pattern1.group(2).capitalize()
            self.logger.debug(f"Pattern 1 found: ${pattern1.group(1)} {pattern1.group(2)}")
            return
        
        # Pattern 2: 80 million (no dollar sign)
        pattern2 = re.search(r'\b(\d+(?:\.\d+)?)\s*(million|billion)\b', text, re.IGNORECASE)
        if pattern2:
            operation['maximum_operation_size'] = pattern2.group(1)
            operation['maximum_operation_multiplier'] = pattern2.group(2).capitalize()
            self.logger.debug(f"Pattern 2 found: {pattern2.group(1)} {pattern2.group(2)}")
            return
        
        # Pattern 3: Find any number and any million/billion separately
        numbers = re.findall(r'\$?(\d+(?:\.\d+)?)', text)
        multipliers = re.findall(r'\b(million|billion)\b', text, re.IGNORECASE)
        
        if numbers and multipliers:
            for num in numbers:
                try:
                    amount = float(num)
                    if 1 <= amount <= 10000:
                        operation['maximum_operation_size'] = num
                        operation['maximum_operation_multiplier'] = multipliers[0].capitalize()
                        self.logger.debug(f"Pattern 3 found: {num} {multipliers[0]}")
                        return
                except ValueError:
                    continue
        
        # Pattern 4: Extract from likely column positions
        self.extract_from_columns(text, operation)
        
        # Pattern 5: Just find a reasonable number and assume Million
        if not operation.get('maximum_operation_size'):
            number_matches = re.findall(r'\b(\d+(?:\.\d+)?)\b', text)
            for num in number_matches:
                try:
                    amount = float(num)
                    if 10 <= amount <= 1000:
                        operation['maximum_operation_size'] = num
                        operation['maximum_operation_multiplier'] = 'Million'
                        self.logger.debug(f"Pattern 5 assumed: {num} Million")
                        return
                except ValueError:
                    continue
        
        # Final check: if we have size but no multiplier, assume Million for reasonable amounts
        if operation.get('maximum_operation_size') and not operation.get('maximum_operation_multiplier'):
            try:
                size_val = float(operation['maximum_operation_size'])
                if 1 <= size_val <= 10000:
                    operation['maximum_operation_multiplier'] = 'Million'
                    self.logger.debug(f"FINAL: Set Million for size {size_val}")
            except (ValueError, TypeError):
                pass
    
    def extract_from_columns(self, text: str, operation: dict):
        """Extract from likely column positions"""
        parts = text.split()
        
        for i, part in enumerate(parts):
            dollar_match = re.match(r'\$(\d+(?:\.\d+)?)', part)
            if dollar_match:
                amount = dollar_match.group(1)
                for j in range(i+1, min(i+3, len(parts))):
                    if parts[j].lower() in ['million', 'billion']:
                        operation['maximum_operation_size'] = amount
                        operation['maximum_operation_multiplier'] = parts[j].capitalize()
                        self.logger.debug(f"Column extraction: ${amount} {parts[j]}")
                        return
                
                try:
                    amt_val = float(amount)
                    if 1 <= amt_val <= 10000:
                        operation['maximum_operation_size'] = amount
                        operation['maximum_operation_multiplier'] = 'Million'
                        self.logger.debug(f"Column default: ${amount} Million")
                        return
                except ValueError:
                    pass
    
    def enhance_operation(self, text: str, operation: dict):
        """Clean up and enhance operation data"""
        # Enhanced operation type parsing
        if operation['operation_type']:
            op_type = operation['operation_type']
            operation['operation_type'] = ' '.join(op_type.split())
        
        if not operation.get('operation_type') or len(operation['operation_type'].strip()) < 3:
            self.extract_operation_type_from_text(text, operation)
        
        # Enhanced security type parsing
        if operation['security_type_and_maturity']:
            sec_type = operation['security_type_and_maturity']
            operation['security_type_and_maturity'] = ' '.join(sec_type.split())
        
        if not operation.get('security_type_and_maturity') or len(operation['security_type_and_maturity'].strip()) < 3:
            self.extract_security_type_from_text(text, operation)
        
        # Clean time format
        if operation['operation_time']:
            time_str = operation['operation_time']
            time_clean = re.sub(r'(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})\s*(am|pm)', 
                               r'\1\3-\2\3', time_str, flags=re.IGNORECASE)
            operation['operation_time'] = time_clean
    
    def extract_operation_type_from_text(self, text: str, operation: dict):
        """Extract operation type from full text with multiple patterns"""
        operation_patterns = [
            r'(Small Value Purchase)', r'(Small Value Sale)',
            r'(Small-Value Purchase)', r'(Small-Value Sale)',
            r'(Large Value Purchase)', r'(Large Value Sale)',
            r'(Large-Value Purchase)', r'(Large-Value Sale)',
            r'(Outright Purchase)', r'(Outright Sale)',
            r'(OutrightPurchase)', r'(OutrightSale)',
            r'(Reinvestment Purchase)', r'(Reinvestment Sale)', r'(Reinvestment)',
            r'(Reserve Management Purchase)', r'(Reserve Management Sale)', r'(Reserve Management)',
            r'(Purchase)', r'(Sale)', r'(Buy)', r'(Sell)',
            r'(Reverse Repo)', r'(Reverse Repurchase)', r'(Repo)', r'(Repurchase)',
            r'(Term Repo)', r'(Overnight Repo)', r'(Roll)', r'(Rollover)',
            r'(Maturity Extension)', r'(Extension)', r'(Exchange)', r'(Swap)',
            r'(Auction Purchase)', r'(Auction Sale)', r'(Competitive Auction)', r'(Non-competitive Auction)',
            r'(Settlement Purchase)', r'(Settlement Sale)', r'(Redemption)', r'(Issuance)',
            r'(Small Value Purchase Purchase)', r'(Small Value Sale Sale)',
            r'(Operation\s+\w+)', r'(\w+\s+Operation)',
        ]
        
        for pattern in operation_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                operation_text = match.group(1).strip()
                operation_text = re.sub(r'\s+', ' ', operation_text)
                operation_text = operation_text.replace('  ', ' ')
                
                if operation_text.lower() in text.lower():
                    case_match = re.search(re.escape(operation_text), text, re.IGNORECASE)
                    if case_match:
                        operation_text = case_match.group(0)
                
                if len(operation_text) > 2:
                    operation['operation_type'] = operation_text
                    self.logger.debug(f"Extracted operation type: {operation_text}")
                    return
        
        # Fallback patterns
        fallback_patterns = [
            r'\b(purchase|buy|acquisition)\b',
            r'\b(sale|sell|disposition)\b'
        ]
        
        for pattern in fallback_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                word = match.group(1)
                if word.lower() in ['purchase', 'buy', 'acquisition']:
                    operation['operation_type'] = 'Purchase'
                elif word.lower() in ['sale', 'sell', 'disposition']:
                    operation['operation_type'] = 'Sale'
                self.logger.debug(f"Fallback operation type: {operation['operation_type']}")
                return
    
    def extract_security_type_from_text(self, text: str, operation: dict):
        """Extract security type from full text with multiple patterns"""
        security_patterns = [
            # Treasury Bills variations
            r'(Treasury Bills?\s+[^,\n]*)', r'(Bills?\s+\d+[^,\n]*)', r'(BILL\s+[^,\n]*)',
            
            # Treasury Coupons variations
            r'(Treasury Coupons?\s+[^,\n]*)', r'(Treasury coupons?\s+[^,\n]*)', r'(Coupons?\s+\d+[^,\n]*)',
            r'(TreasuryCoupons[^,\n]*)', r'(Treasury [Cc]oupons?\s+\d+(?:\.\d+)?\*?\s+to\s+\d+(?:\.\d+)?\s+year\s+sector)',
            
            # TIPS variations
            r'(TIPS\s+[^,\n]*)', r'(Treasury Inflation-Protected Securities\s+[^,\n]*)',
            r'(Inflation-Protected Securities\s+[^,\n]*)', r'(TIPS\s+\d+(?:\.\d+)?\s+to\s+\d+(?:\.\d+)?\s+year\s+sector)',
            
            # Treasury FRNs
            r'(Treasury FRNs?\s+[^,\n]*)', r'(FRNs?\s+[^,\n]*)', r'(Floating Rate Notes?\s+[^,\n]*)',
            
            # Treasury Notes/Bonds
            r'(Treasury Notes?\s+[^,\n]*)', r'(Notes?\s+\d+[^,\n]*)',
            r'(Treasury Bonds?\s+[^,\n]*)', r'(Bonds?\s+\d+[^,\n]*)',
            
            # NOMINAL securities
            r'(NOMINAL\s+[^,\n]*)',
            
            # Treasury Bills with year sector
            r'(Treasury Bills?\s+\d+(?:\.\d+)?\*?\s+to\s+\d+(?:\.\d+)?\s+year\s+sector)',
            
            # STRIPS
            r'(STRIPS\s+[^,\n]*)', r'(Separate Trading[^,\n]*)',
            
            # Generic Treasury Securities
            r'(Treasury Securities\s+[^,\n]*)', r'(Government Securities\s+[^,\n]*)',
            
            # Concatenated versions
            r'(TreasuryBills[^,\n]*)', r'(TreasuryFRNs[^,\n]*)', r'(TIPS\d+[^,\n]*)',
            
            # Specific maturity range patterns
            r'(Treasury Coupons?\s*\d+(?:\.\d+)?\*?\s+to\s+\d+(?:\.\d+)?)',
            r'(Treasury Bills?\s*\d+\*?\s+to\s+\d+)', r'(TIPS\s*\d+(?:\.\d+)?\s+to\s+\d+(?:\.\d+)?)',
            r'(Treasury FRNs?\s*\d+\*?\s+to\s+\d+)',
            
            # Any security with maturity range pattern
            r'([A-Za-z\s]+\d+(?:\.\d+)?\*?\s+to\s+\d+(?:\.\d+)?[^,\n]*)',
        ]
        
        for pattern in security_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                security_text = match.group(1).strip()
                security_text = re.sub(r'\s+', ' ', security_text)
                security_text = security_text.replace('  ', ' ')
                
                if len(security_text) > 3 and any(char.isalpha() for char in security_text):
                    operation['security_type_and_maturity'] = security_text
                    self.logger.debug(f"Extracted security type: {security_text}")
                    return
        
        # Fallback: look for any text with maturity pattern
        maturity_with_text = re.search(r'([A-Za-z][A-Za-z\s]*)\s+(\d+(?:\.\d+)?\*?\s+to\s+\d+(?:\.\d+)?)', text)
        if maturity_with_text:
            security_name = maturity_with_text.group(1).strip()
            maturity_range = maturity_with_text.group(2).strip()
            
            if security_name.lower() not in ['and', 'the', 'of', 'to', 'in', 'for', 'with']:
                operation['security_type_and_maturity'] = security_name + " " + maturity_range
                self.logger.debug(f"Fallback security type: {security_name} {maturity_range}")
    
    def calculate_release_date(self, operation: dict) -> str:
        """Calculate release date"""
        if operation['operation_date']:
            try:
                date_obj = datetime.strptime(operation['operation_date'], '%m/%d/%Y')
                return date_obj.strftime('%Y%m%d')
            except ValueError:
                pass
        
        if operation['settlement_date']:
            try:
                date_obj = datetime.strptime(operation['settlement_date'], '%m/%d/%Y')
                return date_obj.strftime('%Y%m%d')
            except ValueError:
                pass
        
        return datetime.now().strftime('%Y%m%d')
    
    def is_valid_operation(self, operation: dict) -> bool:
        """Check if operation is valid"""
        has_date = bool(operation.get('operation_date'))
        has_type = bool(operation.get('operation_type'))
        has_security = bool(operation.get('security_type_and_maturity'))
        
        return has_date and (has_type or has_security)
    
    def parse_text_fallback(self, text: str) -> List[Dict]:
        """Fallback text parsing"""
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
                if current_operation and current_operation.get('operation_date'):
                    operations.append(self.complete_operation(current_operation))
                
                current_operation = {'operation_date': date_match.group(1)}
                self.extract_line_data(line, current_operation)
            
            elif current_operation:
                self.extract_line_data(line, current_operation)
        
        if current_operation and current_operation.get('operation_date'):
            operations.append(self.complete_operation(current_operation))
        
        return operations
    
    def extract_line_data(self, line: str, operation: dict):
        """Extract data from text line"""
        # Time
        if not operation.get('operation_time'):
            time_match = re.search(r'(\d{1,2}:\d{2}[ap]m[-–]\d{1,2}:\d{2}[ap]m)', line, re.IGNORECASE)
            if time_match:
                operation['operation_time'] = time_match.group(1)
        
        # Settlement date
        if not operation.get('settlement_date'):
            dates = re.findall(r'\d{1,2}/\d{1,2}/\d{4}', line)
            if len(dates) > 1:
                operation['settlement_date'] = dates[1]
        
        # Operation type
        if not operation.get('operation_type'):
            if 'Small Value Purchase' in line:
                operation['operation_type'] = 'Small Value Purchase'
            elif 'Small Value Sale' in line:
                operation['operation_type'] = 'Small Value Sale'
            elif 'Purchase' in line:
                operation['operation_type'] = 'Purchase'
            elif 'Sale' in line:
                operation['operation_type'] = 'Sale'
        
        # Security type
        if not operation.get('security_type_and_maturity'):
            if 'Treasury Bills' in line:
                operation['security_type_and_maturity'] = 'Bill'
            elif 'TIPS' in line:
                operation['security_type_and_maturity'] = 'TIPS'
            elif 'Treasury Coupons' in line or 'Coupons' in line:
                operation['security_type_and_maturity'] = 'Nominal'
        
        # Amount
        if not operation.get('maximum_operation_size'):
            self.extract_amount(line, operation)
    
    def complete_operation(self, operation: dict) -> dict:
        """Complete operation with all fields"""
        return {
            'operation_date': operation.get('operation_date', ''),
            'operation_time': operation.get('operation_time', ''),
            'settlement_date': operation.get('settlement_date', ''),
            'operation_type': operation.get('operation_type', ''),
            'security_type_and_maturity': operation.get('security_type_and_maturity', ''),
            'maturity_range': operation.get('maturity_range', ''),
            'maximum_operation_currency': operation.get('maximum_operation_currency', '$'),
            'maximum_operation_size': operation.get('maximum_operation_size', ''),
            'maximum_operation_multiplier': operation.get('maximum_operation_multiplier', ''),
            'release_date': self.calculate_release_date(operation)
        }

class SmartFEDAOExtractor:
    """Enhanced FEDAO extractor with specialized FRBNY parser integration."""

    def __init__(self, project_id: str, region: str):
        self.logger = logging.getLogger(__name__)
        
        # Initialize the Vertex AI client (keeping for compatibility)
        aiplatform.init(project=project_id, location=region)
        self.model = GenerativeModel("gemini-2.0-flash")
        
        # Initialize our specialized FRBNY parser
        self.frbny_parser = EnhancedFRBNYParser(self.logger)
        
        # Initialize validator
        self.validator = ExtractionValidator(self.logger)

    def smart_html_preprocessing(self, html_content: str) -> List[str]:
        """Extract PDF links from HTML with configurable scope"""
        soup = BeautifulSoup(html_content, 'html.parser')
        pdf_links = []
        
        # Get extraction scope from environment variable
        extraction_scope = os.environ.get("EXTRACTION_SCOPE", "tables").lower()
        
        if extraction_scope == "tables":
            # Only look for PDFs within table elements
            tables = soup.find_all('table')
            
            for table in tables:
                links = table.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if href.lower().endswith('.pdf'):
                        if href.startswith('/'):
                            href = 'https://www.newyorkfed.org' + href
                        elif not href.startswith('http'):
                            href = 'https://www.newyorkfed.org/' + href
                        pdf_links.append(href)
        
        elif extraction_scope == "full":
            # Look for PDFs anywhere on the page
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.lower().endswith('.pdf'):
                    if href.startswith('/'):
                        href = 'https://www.newyorkfed.org' + href
                    elif not href.startswith('http'):
                        href = 'https://www.newyorkfed.org/' + href
                    pdf_links.append(href)
        
        elif extraction_scope == "specific":
            # Look for PDFs only in specific containers
            containers = soup.find_all(['div', 'section'], class_=lambda x: x and ('schedule' in x.lower() or 'operation' in x.lower()))
            
            for container in containers:
                links = container.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if href.lower().endswith('.pdf'):
                        if href.startswith('/'):
                            href = 'https://www.newyorkfed.org' + href
                        elif not href.startswith('http'):
                            href = 'https://www.newyorkfed.org/' + href
                        pdf_links.append(href)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_pdf_links = []
        for link in pdf_links:
            if link not in seen:
                seen.add(link)
                unique_pdf_links.append(link)
        
        return unique_pdf_links

    def extract_data_from_pdf(self, pdf_url: str) -> List[Dict]:
        """Extract FRBNY data from PDF using our specialized parser"""
        try:
            self.logger.info(f"Downloading and parsing PDF: {pdf_url}")
            
            # Download the PDF
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            # Use our specialized FRBNY parser
            operations = self.frbny_parser.parse_pdf_content(response.content, pdf_url)
            
            if operations:
                self.logger.info(f"Successfully extracted {len(operations)} operations from {pdf_url}")
                
                # Log sample operation for verification
                if operations:
                    sample = operations[0]
                    self.logger.info(f"Sample operation: Date={sample.get('operation_date')}, "
                                   f"Type={sample.get('operation_type')}, "
                                   f"Amount=${sample.get('maximum_operation_size')} {sample.get('maximum_operation_multiplier')}")
                
                return operations
            else:
                self.logger.warning(f"No operations extracted from PDF: {pdf_url}")
                return []
            
        except Exception as e:
            self.logger.error(f"Error extracting data from PDF {pdf_url}: {e}")
            return []

    def extract_with_ai_enhanced(self, html_content: str, table_type: str) -> List[Dict]:
        """Enhanced extraction using our specialized FRBNY parser instead of generic AI."""
        pdf_links = self.smart_html_preprocessing(html_content)
        
        if not pdf_links:
            self.logger.warning("No PDF links found in HTML content")
            return []

        # TEST MODE: Process only first PDF when enabled
        test_mode = os.environ.get("TEST_MODE", "true").lower() == "true"
        max_pdfs = 1 if test_mode else 3
        
        all_extracted_data = []
        
        # Process limited number of PDFs based on test mode
        for i, pdf_url in enumerate(pdf_links[:max_pdfs]):
            self.logger.info(f"Processing PDF {i+1}/{min(len(pdf_links), max_pdfs)}: {pdf_url}")
            
            # Use our specialized FRBNY parser
            pdf_operations = self.extract_data_from_pdf(pdf_url)
            
            if pdf_operations:
                self.logger.info(f"Extracted {len(pdf_operations)} operations from PDF")
                all_extracted_data.extend(pdf_operations)
            else:
                self.logger.warning(f"No operations extracted from PDF: {pdf_url}")
            
            # In test mode, stop after first PDF
            if test_mode:
                break

        self.logger.info(f"Total extracted data: {len(all_extracted_data)} operations")
        
        # Log sample of extracted data for verification
        if all_extracted_data:
            sample = all_extracted_data[0]
            self.logger.info(f"Sample extracted record: {sample}")
            
            # Verify critical fields are present
            has_amount_size = bool(sample.get('maximum_operation_size'))
            has_amount_multiplier = bool(sample.get('maximum_operation_multiplier'))
            self.logger.info(f"Amount extraction verification: size={has_amount_size}, multiplier={has_amount_multiplier}")
        
        return all_extracted_data

    def post_process_extracted_data(self, data: List[Dict], table_type: str) -> List[Dict]:
        """Post-process extracted data according to runbook requirements"""
        if not data:
            return data
            
        processed_data = []

        for row in data:
            processed_row = row.copy()
            
            # Clean up all text fields to remove indentation characters and commas
            for field_name, field_value in processed_row.items():
                if field_value and isinstance(field_value, str):
                    # Remove indentation characters and commas as per runbook
                    cleaned_value = re.sub(r'[,\t\n\r]', ' ', str(field_value))
                    cleaned_value = ' '.join(cleaned_value.split())  # Remove extra spaces
                    processed_row[field_name] = cleaned_value
            
            # Ensure amount fields are properly set
            if not processed_row.get('maximum_operation_multiplier') and processed_row.get('maximum_operation_size'):
                try:
                    size = float(processed_row['maximum_operation_size'])
                    if 1 <= size <= 10000:  # Reasonable range for millions
                        processed_row['maximum_operation_multiplier'] = 'Million'
                        self.logger.debug(f"Set default Million multiplier for size: {size}")
                except (ValueError, TypeError):
                    pass
            
            processed_data.append(processed_row)
        
        return processed_data


def scrape_treasury_operations_enhanced_ai(treasury_url: str, extractor: SmartFEDAOExtractor) -> List[Dict]:
    """Enhanced Treasury operations scraping using specialized FRBNY parser."""
    renderer_service_url = os.environ.get("RENDERER_SERVICE_URL", "http://your-renderer-service-url.a.run.app/render")

    render_config = {
        "url": treasury_url,
        "interactions_config": {
            "activate_tabs": True
        }
    }

    try:
        response = requests.post(renderer_service_url, json=render_config, timeout=60)
        response.raise_for_status()
        render_result = response.json()

        if 'error' in render_result or 'html' not in render_result:
            error_message = render_result.get('error', 'Unknown error from renderer')
            extractor.logger.error(f"Renderer service failed for {treasury_url}: {error_message}")
            return []

        html_content = render_result['html']

    except requests.exceptions.RequestException as e:
        extractor.logger.error(f"Failed to call renderer service for {treasury_url}: {e}")
        return []

    # Extract using our enhanced FRBNY parser
    moa_data = extractor.extract_with_ai_enhanced(html_content, "MOA")
    standardized_moa = extractor.post_process_extracted_data(moa_data, "MOA")
    
    return standardized_moa

def scrape_ambs_operations_enhanced_ai(ambs_url: str, extractor: SmartFEDAOExtractor) -> List[Dict]:
    """Enhanced AMBS operations scraping using specialized FRBNY parser."""
    renderer_service_url = os.environ.get("RENDERER_SERVICE_URL", "http://your-renderer-service-url.a.run.app/render")

    render_config = {
        "url": ambs_url,
        "interactions_config": {
            "activate_tabs": True
        }
    }
    
    try:
        response = requests.post(renderer_service_url, json=render_config, timeout=60)
        response.raise_for_status()
        render_result = response.json()

        if 'error' in render_result or 'html' not in render_result:
            error_message = render_result.get('error', 'Unknown error from renderer')
            extractor.logger.error(f"Renderer service failed for {ambs_url}: {error_message}")
            return []

        html_content = render_result['html']

    except requests.exceptions.RequestException as e:
        extractor.logger.error(f"Failed to call renderer service for {ambs_url}: {e}")
        return []

    # Extract using our enhanced FRBNY parser
    toa_data = extractor.extract_with_ai_enhanced(html_content, "TOA")
    standardized_toa = extractor.post_process_extracted_data(toa_data, "TOA")
    
    return standardized_toa


def upload_csv_to_gcs(csv_content: str, bucket_name: str, file_path: str, active_logger) -> str:
    """Upload CSV content to GCS"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.upload_from_string(csv_content, content_type='text/csv')
        
        return f"gs://{bucket_name}/{file_path}"
    except Exception as e:
        active_logger.error(f"Error uploading CSV to GCS gs://{bucket_name}/{file_path}: {str(e)}")
        raise


@functions_framework.cloud_event
def scrape_fedao_sources_ai(cloud_event):
    """AI-powered FEDAO source scraping with specialized FRBNY parser for accurate field extraction."""
    active_logger = setup_logging("simba", "fedao_project")
    active_logger.warning("Starting FEDAO source scraping with specialized FRBNY parser")

    try:
        customer_config = load_customer_config("simba")
        gcp_project_id = customer_config.get("gcp_project_id", os.environ.get("GCP_PROJECT"))
        region = os.environ.get("FUNCTION_REGION", "europe-west1")

        # Initialize the enhanced extractor with our specialized FRBNY parser
        extractor = SmartFEDAOExtractor(
            project_id=gcp_project_id, 
            region=region
        )

        treasury_url = os.environ.get("FEDAO_TREASURY_URL",
                                      "https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details")
        ambs_url = os.environ.get("FEDAO_AMBS_URL",
                                  "https://www.newyorkfed.org/markets/ambs_operation_schedule#tabs-2")
        output_bucket = os.environ.get("FEDAO_OUTPUT_BUCKET", "execo-simba-fedao-poc")

        # --- MOA Data Extraction (Treasury Operations) ---
        active_logger.info("Starting MOA data extraction using specialized FRBNY parser")
        moa_data = scrape_treasury_operations_enhanced_ai(treasury_url, extractor)
        moa_raw_file_path = f"FEDAO/inputs/raw_manual_uploads/FEDAO_MOA_RAW_DATA.csv"
        moa_file_url = ""

        if moa_data:
            moa_df = pd.DataFrame(moa_data)
            current_date = datetime.now().strftime('%Y%m%d')
            moa_df['Source_Date'] = current_date
            
            # Log field extraction success
            amount_fields_present = sum(1 for row in moa_data 
                                      if row.get('maximum_operation_size') and row.get('maximum_operation_multiplier'))
            active_logger.info(f"MOA extraction results: {len(moa_data)} total operations, "
                             f"{amount_fields_present} with complete amount fields")
            
            moa_csv_content = moa_df.to_csv(index=False)
            moa_file_url = upload_csv_to_gcs(moa_csv_content, output_bucket, moa_raw_file_path, active_logger)
            active_logger.info(f"MOA data uploaded: {len(moa_data)} rows with headers: {list(moa_df.columns)}")

        # --- TOA Data Extraction (AMBS Operations) ---
        active_logger.info("Starting TOA data extraction using specialized FRBNY parser")
        toa_data = scrape_ambs_operations_enhanced_ai(ambs_url, extractor)
        toa_raw_file_path = f"FEDAO/inputs/raw_manual_uploads/FEDAO_TOA_RAW_DATA.csv"
        toa_file_url = ""

        if toa_data:
            toa_df = pd.DataFrame(toa_data)
            current_date = datetime.now().strftime('%Y%m%d')
            toa_df['Source_Date'] = current_date
            
            # Log field extraction success
            amount_fields_present = sum(1 for row in toa_data 
                                      if row.get('maximum_operation_size') and row.get('maximum_operation_multiplier'))
            active_logger.info(f"TOA extraction results: {len(toa_data)} total operations, "
                             f"{amount_fields_present} with complete amount fields")
            
            toa_csv_content = toa_df.to_csv(index=False)
            toa_file_url = upload_csv_to_gcs(toa_csv_content, output_bucket, toa_raw_file_path, active_logger)
            active_logger.info(f"TOA data uploaded: {len(toa_data)} rows with headers: {list(toa_df.columns)}")

        # Final summary with field extraction metrics
        total_operations = len(moa_data) + len(toa_data) if moa_data and toa_data else (len(moa_data) if moa_data else len(toa_data) if toa_data else 0)
        total_with_amounts = 0
        
        if moa_data:
            total_with_amounts += sum(1 for row in moa_data 
                                    if row.get('maximum_operation_size') and row.get('maximum_operation_multiplier'))
        if toa_data:
            total_with_amounts += sum(1 for row in toa_data 
                                    if row.get('maximum_operation_size') and row.get('maximum_operation_multiplier'))

        active_logger.warning(f"FEDAO extraction complete with specialized parser: "
                            f"Total {total_operations} operations, {total_with_amounts} with complete amount fields "
                            f"({(total_with_amounts/total_operations*100):.1f}% success rate)")
        
        return {
            "status": "success",
            "method": "Specialized FRBNY parser with enhanced field extraction",
            "moa_rows_collected": len(moa_data) if moa_data else 0,
            "toa_rows_collected": len(toa_data) if toa_data else 0,
            "total_operations": total_operations,
            "operations_with_complete_amounts": total_with_amounts,
            "amount_extraction_success_rate": f"{(total_with_amounts/total_operations*100):.1f}%" if total_operations > 0 else "0%",
            "moa_raw_file": moa_file_url if moa_file_url else "No MOA file generated",
            "toa_raw_file": toa_file_url if toa_file_url else "No TOA file generated"
        }

    except Exception as e:
        active_logger.error(f"Critical error in enhanced FEDAO scraping: {str(e)}", exc_info=True)
        raise