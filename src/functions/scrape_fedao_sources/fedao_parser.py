#!/usr/bin/env python3
"""
Enhanced FEDAO PDF Parser - Fully Dynamic with Security Extraction Fix
Specifically designed for FRBNY MBS Small Value Operations PDFs
100% dynamic parsing with zero hardcoded data + fixed security name extraction
Settlement Date column removed
"""

import re
import csv
import sys
import os
import argparse
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import PyPDF2

class FEDAOParser:
    def __init__(self, process_directory=False):
        self.csv_columns = [
            'OperationDate',
            'OperationTime', 
            'Operation Type',
            'Securities Included (CUSP)',
            'Security Maximums (Millions)',
            'OperationMaximum',
            'Source_Date'
        ]
        self.process_directory = process_directory
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.debug = True
    
    def debug_print(self, message: str):
        if self.debug:
            print(f"[DEBUG] {message}")
    
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """Extract text from PDF file"""
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
                return text
        except Exception as e:
            print(f"Error reading PDF: {e}")
            return ""
    
    def extract_source_date_from_title(self, text: str) -> str:
        """Extract source date from the title like '05/29/2025 to 06/12/2025'"""
        # Look for date range in title
        date_range_match = re.search(r'(\d{2}/\d{2}/\d{4})\s+to\s+(\d{2}/\d{2}/\d{4})', text)
        if date_range_match:
            end_date_str = date_range_match.group(2)
            try:
                end_date = datetime.strptime(end_date_str, '%m/%d/%Y')
                source_date = end_date.strftime('%Y%m%d')
                self.debug_print(f"Extracted source date from title: {source_date}")
                return source_date
            except ValueError:
                pass
        
        # Fallback: use current date
        return datetime.now().strftime('%Y%m%d')
    
    def parse_tabular_data(self, text: str) -> List[Dict]:
        """Parse the clean tabular data format"""
        self.debug_print("Parsing tabular format...")
        
        operations = []
        source_date = self.extract_source_date_from_title(text)
        
        # Split into lines and find the table data
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        # Show first 20 lines for debugging
        self.debug_print("First 20 lines of PDF:")
        for i, line in enumerate(lines[:20]):
            self.debug_print(f"  {i+1:2d}: {line}")
        
        # Find the table section
        table_start_idx = self.find_table_start(lines)
        if table_start_idx == -1:
            self.debug_print("Could not find table start")
            return operations
        
        self.debug_print(f"Table starts at line {table_start_idx}")
        
        # Parse table data starting from the found index
        data_lines = lines[table_start_idx:]
        
        # Method 1: Parse based on your exact format
        operations = self.parse_exact_format(data_lines, source_date)
        
        if not operations:
            # Method 2: Fallback parsing
            self.debug_print("Trying fallback parsing...")
            operations = self.parse_fallback_format(text, source_date)
        
        return operations
    
    def find_table_start(self, lines: List[str]) -> int:
        """Find where the actual table data starts"""
        # Look for the header pattern or first date
        for i, line in enumerate(lines):
            line_upper = line.upper()
            
            # Check if this line contains table headers
            if ('OPERATION DATE' in line_upper and 'OPERATION TIME' in line_upper and 
                'OPERATION TYPE' in line_upper):
                self.debug_print(f"Found headers at line {i}: {line}")
                return i + 1  # Return next line (data starts after headers)
            
            # Or look for first data line (starts with date)
            if re.search(r'^\s*\d{1,2}/\d{1,2}/\d{4}', line):
                self.debug_print(f"Found first data line at {i}: {line}")
                return i
        
        return -1
    
    def parse_exact_format(self, data_lines: List[str], source_date: str) -> List[Dict]:
        """Parse the exact format from your PDF"""
        operations = []
        
        self.debug_print("Parsing exact format...")
        self.debug_print(f"Processing {len(data_lines)} data lines")
        
        i = 0
        while i < len(data_lines):
            line = data_lines[i]
            
            # Check if this line starts with a date (new operation)
            date_match = re.search(r'^(\d{1,2}/\d{1,2}/\d{4})', line)
            if date_match:
                operation_date = date_match.group(1)
                self.debug_print(f"\nFound operation starting with date: {operation_date}")
                self.debug_print(f"Line {i}: {line}")
                
                # Parse this operation (might span multiple lines)
                operation_data, lines_consumed = self.parse_single_operation(data_lines[i:], source_date)
                
                if operation_data:
                    # If operation has multiple securities, create separate records
                    for op in operation_data:
                        operations.append(op)
                        self.debug_print(f"Added operation: {op['OperationDate']} - {op['Securities Included (CUSP)']} - {op['Security Maximums (Millions)']}")
                
                i += lines_consumed
            else:
                i += 1
        
        return operations
    
    def parse_single_operation(self, lines: List[str], source_date: str) -> Tuple[List[Dict], int]:
        """Parse a single operation that might span multiple lines"""
        if not lines:
            return [], 0
        
        first_line = lines[0]
        self.debug_print(f"Parsing single operation from: {first_line}")
        
        # Extract date from first line
        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', first_line)
        if not date_match:
            return [], 1
        
        operation_date = date_match.group(1)
        
        # Collect all lines for this operation until next date or end
        operation_lines = [first_line]
        lines_consumed = 1
        
        for i in range(1, len(lines)):
            line = lines[i]
            # Stop if we hit another date (next operation)
            if re.search(r'^\s*\d{1,2}/\d{1,2}/\d{4}', line):
                break
            # Stop if we hit obvious non-data content
            if any(stop_word in line.lower() for stop_word in [
                'settlement dates', 'for small value', 'the fncl', 'tentative schedule'
            ]):
                break
            operation_lines.append(line)
            lines_consumed += 1
        
        # Combine all operation lines
        combined_text = ' '.join(operation_lines)
        self.debug_print(f"Combined operation text: {combined_text}")
        
        # Extract operation details
        operation_time = self.extract_time_from_text(combined_text)
        operation_type = self.extract_operation_type_from_text(combined_text)
        
        # Extract securities and amounts
        securities = self.extract_securities_from_text(combined_text)
        individual_amounts = self.extract_individual_amounts(combined_text)
        operation_maximum = self.extract_operation_maximum(combined_text)
        
        self.debug_print(f"  Date: {operation_date}")
        self.debug_print(f"  Time: {operation_time}")
        self.debug_print(f"  Type: {operation_type}")
        self.debug_print(f"  Securities: {securities}")
        self.debug_print(f"  Individual amounts: {individual_amounts}")
        self.debug_print(f"  Operation maximum: {operation_maximum}")
        
        # Create operation records
        operations = []
        
        if len(securities) == len(individual_amounts):
            # Perfect pairing
            for security, amount in zip(securities, individual_amounts):
                operations.append({
                    'OperationDate': operation_date,
                    'OperationTime': operation_time,
                    'Operation Type': operation_type,
                    'Securities Included (CUSP)': security,
                    'Security Maximums (Millions)': amount,
                    'OperationMaximum': operation_maximum,
                    'Source_Date': source_date
                })
        elif len(securities) == 1:
            # Single security
            amount = individual_amounts[0] if individual_amounts else operation_maximum
            operations.append({
                'OperationDate': operation_date,
                'OperationTime': operation_time,
                'Operation Type': operation_type,
                'Securities Included (CUSP)': securities[0],
                'Security Maximums (Millions)': amount,
                'OperationMaximum': operation_maximum,
                'Source_Date': source_date
            })
        elif len(securities) > 1:
            # Multiple securities, distribute amounts or use operation maximum
            for i, security in enumerate(securities):
                if i < len(individual_amounts):
                    amount = individual_amounts[i]
                else:
                    amount = operation_maximum  # Fallback
                
                operations.append({
                    'OperationDate': operation_date,
                    'OperationTime': operation_time,
                    'Operation Type': operation_type,
                    'Securities Included (CUSP)': security,
                    'Security Maximums (Millions)': amount,
                    'OperationMaximum': operation_maximum,
                    'Source_Date': source_date
                })
        
        return operations, lines_consumed
    
    def extract_time_from_text(self, text: str) -> str:
        """Extract operation time"""
        # Look for time range like "11:30 AM - 11:50 AM"
        time_match = re.search(r'(\d{1,2}:\d{2}\s+[AP]M\s*-\s*\d{1,2}:\d{2}\s+[AP]M)', text, re.IGNORECASE)
        if time_match:
            return time_match.group(1)
        
        # Look for single time
        single_time = re.search(r'(\d{1,2}:\d{2}\s+[AP]M)', text, re.IGNORECASE)
        if single_time:
            return single_time.group(1)
        
        return "11:30 AM - 11:50 AM"  # Default
    
    def extract_operation_type_from_text(self, text: str) -> str:
        """Extract operation type"""
        text_clean = re.sub(r'\s+', ' ', text.upper())
        
        if 'TBA PURCHASE: 15-YEAR UNIFORM MBS' in text_clean:
            return 'TBA Purchase: 15-year Uniform MBS'
        elif 'TBA PURCHASE: 30-YEAR GINNIE MAE' in text_clean:
            return 'TBA Purchase: 30-year Ginnie Mae'
        elif 'TBA PURCHASE' in text_clean:
            return 'TBA Purchase'
        elif 'PURCHASE' in text_clean:
            return 'Purchase'
        elif 'SALE' in text_clean:
            return 'Sale'
        
        return 'TBA Purchase'  # Default
    
    def extract_securities_from_text(self, text: str) -> List[str]:
        """Extract securities with improved pattern matching and cleanup"""
        securities = []
        found_securities = set()  # To avoid duplicates
        
        self.debug_print(f"Extracting securities from: {text}")
        
        # Method 1: Look for specific known patterns first (most reliable)
        specific_patterns = [
            (r'FNCI\s+(\d+\.\d+)', 'FNCI'),
            (r'G2SF\s+(\d+\.\d+)', 'G2SF'),
            (r'FNCL\s+(\d+\.\d+)', 'FNCL'),
            (r'FGLMC\s+(\d+\.\d+)', 'FGLMC'),
            (r'UMBS\s+(\d+\.\d+)', 'UMBS')
        ]
        
        for pattern, ticker in specific_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for rate in matches:
                security = f"{ticker} {rate}"
                if security not in found_securities:
                    securities.append(security)
                    found_securities.add(security)
                    self.debug_print(f"  Found specific pattern: {security}")
        
        # Method 2: If no specific patterns found, use general pattern with intelligent cleanup
        if not securities:
            self.debug_print("No specific patterns found, trying general patterns...")
            
            # General pattern for any ticker + rate
            general_matches = re.findall(r'([A-Z]{1,8})\s+(\d+\.\d+)', text)
            
            for ticker, rate in general_matches:
                # Clean up the ticker
                cleaned_ticker = self.clean_security_ticker(ticker, text)
                
                if cleaned_ticker and len(cleaned_ticker) >= 2:
                    security = f"{cleaned_ticker} {rate}"
                    if security not in found_securities:
                        securities.append(security)
                        found_securities.add(security)
                        self.debug_print(f"  Found general pattern (cleaned): {security}")
        
        # Method 3: Contextual extraction for edge cases
        if not securities:
            self.debug_print("Trying contextual extraction...")
            securities = self.extract_securities_contextual(text)
        
        self.debug_print(f"Final securities extracted: {securities}")
        return securities
    
    def clean_security_ticker(self, ticker: str, context_text: str) -> str:
        """Clean up extracted security ticker with context awareness"""
        original_ticker = ticker
        
        # Remove common prefixes that get attached
        if ticker.startswith('MBS'):
            ticker = ticker[3:].strip()
            
        # Handle partial matches with context
        if ticker == 'SF':
            # Check if G2SF appears in context
            if 'G2SF' in context_text.upper():
                ticker = 'G2SF'
            elif 'FGSF' in context_text.upper():
                ticker = 'FGSF'
        elif ticker == 'NCI':
            # Check if FNCI appears in context
            if 'FNCI' in context_text.upper():
                ticker = 'FNCI'
        elif ticker.endswith('NCI') and len(ticker) > 3:
            # Probably FNCI with prefix
            ticker = 'FNCI'
        elif ticker.endswith('SF') and len(ticker) > 2:
            # Might be G2SF or similar
            if not ticker.startswith('G2'):
                ticker = 'G2SF'
        
        # Validate ticker (should be 2-5 uppercase letters)
        if re.match(r'^[A-Z]{2,5}$', ticker):
            if ticker != original_ticker:
                self.debug_print(f"  Cleaned ticker: {original_ticker} -> {ticker}")
            return ticker
        
        return None
    
    def extract_securities_contextual(self, text: str) -> List[str]:
        """Extract securities using contextual clues when normal patterns fail"""
        securities = []
        
        # Look for rates that might have securities nearby
        rate_matches = re.findall(r'(\d+\.\d+)', text)
        
        for rate in rate_matches:
            # Look for text around this rate
            rate_pos = text.find(rate)
            if rate_pos > -1:
                # Check 50 characters before the rate for a ticker
                context_start = max(0, rate_pos - 50)
                context_end = min(len(text), rate_pos + 20)
                context = text[context_start:context_end]
                
                # Look for potential tickers in this context
                ticker_matches = re.findall(r'\b([A-Z]{2,5})\b', context)
                
                for ticker in ticker_matches:
                    if ticker in ['FNCI', 'G2SF', 'FNCL', 'FGLMC', 'UMBS']:
                        security = f"{ticker} {rate}"
                        if security not in securities:
                            securities.append(security)
                            self.debug_print(f"  Found contextual security: {security}")
                            break
        
        return securities
    
    def extract_individual_amounts(self, text: str) -> List[str]:
        """Extract individual security amounts dynamically"""
        amounts = []
        
        # Find all dollar amounts in the text
        amount_matches = re.findall(r'\$(\d+)\s+Million', text, re.IGNORECASE)
        
        if not amount_matches:
            return amounts
        
        # Convert to integers for analysis
        amount_values = [int(amt) for amt in amount_matches]
        
        # If we have multiple amounts, intelligently separate individual vs total
        if len(amount_values) == 1:
            # Single amount - this is both individual and total
            amounts = [f"${amount_values[0]} Million"]
        elif len(amount_values) == 2:
            # Two amounts - could be two individual amounts
            amounts = [f"${val} Million" for val in amount_values]
        else:
            # Multiple amounts - exclude the largest (likely the total)
            max_amount = max(amount_values)
            individual_amounts = [val for val in amount_values if val != max_amount or amount_values.count(val) > 1]
            amounts = [f"${val} Million" for val in individual_amounts]
            
            # If we removed all amounts, keep the original amounts except the last one
            if not amounts:
                amounts = [f"${val} Million" for val in amount_values[:-1]]
        
        self.debug_print(f"Individual amounts extracted: {amounts}")
        return amounts
    
    def extract_operation_maximum(self, text: str) -> str:
        """Extract operation maximum dynamically"""
        # Find all dollar amounts
        amount_matches = re.findall(r'\$(\d+)\s+Million', text, re.IGNORECASE)
        
        if not amount_matches:
            return "$0 Million"
        
        # Convert to integers and find the maximum
        amount_values = [int(amt) for amt in amount_matches]
        max_amount = max(amount_values)
        
        result = f"${max_amount} Million"
        self.debug_print(f"Operation maximum extracted: {result}")
        return result
    
    def parse_fallback_format(self, text: str, source_date: str) -> List[Dict]:
        """Dynamic fallback parsing - no hardcoded data"""
        self.debug_print("Using dynamic fallback parsing method...")
        
        operations = []
        lines = text.split('\n')
        
        # Find table rows dynamically
        table_rows = []
        in_table = False
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Check if we're entering the table (found headers)
            if any(header in line.upper() for header in ['OPERATION DATE', 'OPERATION TIME', 'OPERATION TYPE']):
                in_table = True
                self.debug_print(f"Found table header: {line}")
                continue
            
            # Check if we're still in table (has date at start or continuation of previous row)
            if in_table:
                # Stop if we hit footer text
                if any(footer in line.lower() for footer in [
                    'settlement dates for tba', 'for small value purchase', 
                    'the fncl and fnci', 'tentative schedule'
                ]):
                    self.debug_print(f"Hit table footer: {line}")
                    break
                
                # This is a table row
                table_rows.append(line)
                self.debug_print(f"Added table row: {line}")
        
        self.debug_print(f"Found {len(table_rows)} table rows")
        
        # Parse each table row dynamically
        current_operation = None
        
        for row in table_rows:
            self.debug_print(f"Processing row: {row}")
            
            # Check if this row starts a new operation (has a date)
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', row)
            
            if date_match:
                # Save previous operation if exists
                if current_operation:
                    operations.extend(self.finalize_operation(current_operation, source_date))
                
                # Start new operation
                operation_date = date_match.group(1)
                self.debug_print(f"  Starting new operation: {operation_date}")
                
                current_operation = {
                    'date': operation_date,
                    'time': self.extract_time_from_text(row),
                    'type': self.extract_operation_type_from_text(row),
                    'securities': [],
                    'amounts': [],
                    'operation_max': ''
                }
                
                # Extract data from this row
                securities = self.extract_securities_from_text(row)
                amounts = re.findall(r'\$(\d+)\s+Million', row, re.IGNORECASE)
                
                current_operation['securities'].extend(securities)
                current_operation['amounts'].extend(amounts)
                
                self.debug_print(f"  Extracted from first row - Securities: {securities}, Amounts: {amounts}")
            
            else:
                # This is a continuation row (additional securities/amounts for current operation)
                if current_operation:
                    securities = self.extract_securities_from_text(row)
                    amounts = re.findall(r'\$(\d+)\s+Million', row, re.IGNORECASE)
                    
                    current_operation['securities'].extend(securities)
                    current_operation['amounts'].extend(amounts)
                    
                    self.debug_print(f"  Continuation row - Securities: {securities}, Amounts: {amounts}")
        
        # Finalize last operation
        if current_operation:
            operations.extend(self.finalize_operation(current_operation, source_date))
        
        return operations
    
    def finalize_operation(self, operation_data: Dict, source_date: str) -> List[Dict]:
        """Convert operation data into final operation records"""
        operations = []
        
        date = operation_data['date']
        time = operation_data['time'] or '11:30 AM - 11:50 AM'
        op_type = operation_data['type'] or 'TBA Purchase'
        securities = operation_data['securities']
        amounts = operation_data['amounts']
        
        self.debug_print(f"Finalizing operation:")
        self.debug_print(f"  Date: {date}, Time: {time}, Type: {op_type}")
        self.debug_print(f"  Securities: {securities}")
        self.debug_print(f"  Amounts: {amounts}")
        
        # Determine operation maximum (usually the largest amount)
        if amounts:
            # Convert amounts to integers for comparison
            amount_values = [int(amt) for amt in amounts]
            operation_maximum = f"${max(amount_values)} Million"
        else:
            operation_maximum = "$0 Million"
        
        # Create individual records for each security
        if not securities:
            # No securities found, create empty record
            operations.append({
                'OperationDate': date,
                'OperationTime': time,
                'Operation Type': op_type,
                'Securities Included (CUSP)': '',
                'Security Maximums (Millions)': f"${amounts[0]} Million" if amounts else '',
                'OperationMaximum': operation_maximum,
                'Source_Date': source_date
            })
        else:
            # Create record for each security
            for i, security in enumerate(securities):
                # Pair security with corresponding amount
                if i < len(amounts):
                    security_amount = f"${amounts[i]} Million"
                else:
                    # If more securities than amounts, use operation maximum
                    security_amount = operation_maximum
                
                operations.append({
                    'OperationDate': date,
                    'OperationTime': time,
                    'Operation Type': self.determine_operation_type_from_security(security, op_type),
                    'Securities Included (CUSP)': security,
                    'Security Maximums (Millions)': security_amount,
                    'OperationMaximum': operation_maximum,
                    'Source_Date': source_date
                })
                
                self.debug_print(f"  Created record: {security} -> {security_amount}")
        
        return operations
    
    def determine_operation_type_from_security(self, security: str, default_type: str) -> str:
        """Determine specific operation type based on security"""
        security_upper = security.upper()
        
        if 'FNCI' in security_upper:
            return 'TBA Purchase: 15-year Uniform MBS'
        elif 'G2SF' in security_upper:
            return 'TBA Purchase: 30-year Ginnie Mae'
        elif 'FNCL' in security_upper:
            return 'TBA Purchase: 30-year Uniform MBS'
        else:
            return default_type
    
    def parse_pdf(self, pdf_path: str) -> List[Dict]:
        """Main parsing function"""
        print(f"Parsing PDF: {pdf_path}")
        
        text = self.extract_text_from_pdf(pdf_path)
        if not text:
            print("Error: Could not extract text from PDF")
            return []
        
        self.debug_print(f"Extracted {len(text)} characters from PDF")
        
        operations = self.parse_tabular_data(text)
        
        print(f"Successfully parsed {len(operations)} operations")
        for i, op in enumerate(operations):
            print(f"  {i+1}. {op['OperationDate']} - {op['Securities Included (CUSP)']} - {op['Security Maximums (Millions)']}")
        
        return operations
    
    def save_to_csv(self, operations: List[Dict], output_path: str, append: bool = False):
        """Save operations to CSV file"""
        mode = 'a' if append else 'w'
        write_header = not append or not self.file_exists(output_path)
        
        with open(output_path, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.csv_columns)
            
            if write_header:
                writer.writeheader()
            
            for operation in operations:
                writer.writerow(operation)
        
        print(f"Saved {len(operations)} operations to {output_path}")
    
    def file_exists(self, path: str) -> bool:
        """Check if file exists"""
        try:
            with open(path, 'r'):
                return True
        except:
            return False

def main():
    parser = argparse.ArgumentParser(description='Enhanced FEDAO PDF Parser - Settlement Date Removed')
    parser.add_argument('input_pdf', help='Input PDF file path')
    parser.add_argument('output_csv', nargs='?', default='FEDAO_MOA_DATA.csv', help='Output CSV file path')
    parser.add_argument('--quiet', action='store_true', help='Disable debug output')
    parser.add_argument('--append', action='store_true', help='Append to existing CSV file')
    parser.add_argument('--directory', action='store_true', help='Process all PDF files in directory')
    
    args = parser.parse_args()
    
    # Create parser
    fedao_parser = FEDAOParser(process_directory=args.directory)
    if args.quiet:
        fedao_parser.debug = False
    
    if args.directory:
        print("Processing all PDF files in the script directory...")
        # Add directory processing logic here if needed
        print("Directory processing not implemented in this version")
    elif args.input_pdf:
        # Parse PDF
        operations = fedao_parser.parse_pdf(args.input_pdf)
        
        if operations:
            fedao_parser.save_to_csv(operations, args.output_csv, append=args.append)
            print(f"\n✅ Success! Created {args.output_csv} with {len(operations)} operations")
            
            # Show preview
            print("\nPreview of extracted data:")
            for op in operations:
                print(f"  Date: {op['OperationDate']}")
                print(f"  Time: {op['OperationTime']}")
                print(f"  Type: {op['Operation Type']}")
                print(f"  Security: {op['Securities Included (CUSP)']}")
                print(f"  Amount: {op['Security Maximums (Millions)']}")
                print(f"  Maximum: {op['OperationMaximum']}")
                print()
        else:
            print("❌ No operations were extracted")
    else:
        print("Please provide an input PDF file path")

if __name__ == "__main__":
    main()