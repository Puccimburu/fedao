#!/usr/bin/env python3
"""
FEDAO PDF Parser Script - Dynamic Version
Parses FRBNY MBS operation schedule PDFs and converts them to FEDAO_MOA_DATA.csv format
NO hardcoded data - extracts actual data from any PDF
"""

import re
import csv
import sys
import os
import argparse
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import PyPDF2
import pandas as pd
import glob

class FEDAOParser:
    def __init__(self, process_directory=False):
        self.csv_columns = [
            'OperationDate',
            'OperationTime', 
            'Operation Type',
            'Settlement Date',
            'Securities Included (CUSP)',
            'Security Maximums (Millions)',
            'OperationMaximum',
            'Source_Date'
        ]
        self.process_directory = process_directory
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
    
    def find_pdfs_in_directory(self) -> List[str]:
        """Find all PDF files in the same directory as the script"""
        pdf_pattern = os.path.join(self.script_dir, "*.pdf")
        pdf_files = glob.glob(pdf_pattern)
        return pdf_files
    
    def process_directory_pdfs(self, output_csv: str = None, append: bool = False) -> int:
        """Process all PDF files in the script directory"""
        pdf_files = self.find_pdfs_in_directory()
        
        if not pdf_files:
            print("No PDF files found in the script directory")
            return 0
        
        if output_csv is None:
            output_csv = os.path.join(self.script_dir, "FEDAO_MOA_DATA.csv")
        
        all_operations = []
        processed_count = 0
        
        print(f"Found {len(pdf_files)} PDF file(s) in directory:")
        for pdf_file in pdf_files:
            print(f"  - {os.path.basename(pdf_file)}")
        
        for pdf_file in pdf_files:
            print(f"\nProcessing: {os.path.basename(pdf_file)}")
            try:
                operations = self.parse_pdf(pdf_file)
                if operations:
                    all_operations.extend(operations)
                    processed_count += 1
                    print(f"  ✓ Extracted {len(operations)} operations")
                    for op in operations:
                        print(f"    - {op['OperationDate']}: {op['Securities Included (CUSP)']} {op['Security Maximums (Millions)']}")
                else:
                    print(f"  ✗ No operations extracted from {os.path.basename(pdf_file)}")
                    # Print debug info
                    text = self.extract_text_from_pdf(pdf_file)
                    if text:
                        print(f"  Debug: PDF text length: {len(text)} characters")
                        pdf_type = self.detect_pdf_type(text)
                        print(f"  Debug: Detected type: {pdf_type}")
                        # Show first 500 characters of text for debugging
                        print(f"  Debug: First 500 chars:")
                        print(f"    {text[:500]}")
                    else:
                        print(f"  Debug: Could not extract text from PDF")
            except Exception as e:
                print(f"  ✗ Error processing {os.path.basename(pdf_file)}: {e}")
                import traceback
                print(f"  Debug: Full error trace:")
                traceback.print_exc()
        
        if all_operations:
            all_operations.sort(key=lambda x: (x['Source_Date'], x['OperationDate'], x['OperationTime']))
            self.save_to_csv(all_operations, output_csv, append=append)
            print(f"\n✓ Successfully processed {processed_count} PDF(s)")
            print(f"✓ Total operations extracted: {len(all_operations)}")
            print(f"✓ Output saved to: {output_csv}")
        else:
            print("\n✗ No operations were extracted from any PDF files")
        
        return processed_count
    
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
    
    def detect_pdf_type(self, text: str) -> str:
        """Detect which type of PDF format we're dealing with"""
        # Check for Small Value Operations indicators
        small_value_indicators = [
            "Small Value Operations",
            "TBA Purchase",
            "Agency Mortgage-Backed Securities",
            "AMBS",
            "Ginnie Mae",
            "Uniform MBS"
        ]
        
        # Check for FedTrade indicators
        fedtrade_indicators = [
            "FedTrade",
            "reinvestment purchases",
            "Federal Reserve Bank of New York",
            "FRBNY"
        ]
        
        text_upper = text.upper()
        
        small_value_score = sum(1 for indicator in small_value_indicators if indicator.upper() in text_upper)
        fedtrade_score = sum(1 for indicator in fedtrade_indicators if indicator.upper() in text_upper)
        
        # Also check for table header patterns
        if re.search(r'OPERATION\s+DATE.*OPERATION\s+TIME.*OPERATION\s+TYPE', text, re.IGNORECASE):
            small_value_score += 2
        elif re.search(r'Operation\s+Date.*Operation\s+Time.*Operation\s+Type', text, re.IGNORECASE):
            fedtrade_score += 2
        
        if small_value_score > fedtrade_score:
            return "small_value"
        elif fedtrade_score > 0:
            return "fedtrade"
        else:
            print("Warning: Could not detect PDF type clearly, assuming small_value format")
            return "small_value"
    
    def parse_small_value_pdf(self, text: str) -> List[Dict]:
        """Parse Small Value Operations PDF - Dynamic extraction"""
        operations = []
        
        print("Parsing Small Value Operations PDF dynamically...")
        
        # Extract the date range from title for source date
        source_date = self.extract_source_date(text)
        
        # Try multiple parsing approaches
        operations = self.parse_table_structure(text, source_date)
        
        if not operations:
            operations = self.parse_line_by_line(text, source_date)
        
        if not operations:
            operations = self.parse_with_patterns(text, source_date)
        
        return operations
    
    def extract_source_date(self, text: str) -> str:
        """Extract source date from PDF text"""
        # Look for date ranges in various formats
        date_patterns = [
            r'(\d{1,2}/\d{1,2}/\d{4})\s+to\s+(\d{1,2}/\d{1,2}/\d{4})',
            r'(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})',
            r'from\s+(\d{1,2}/\d{1,2}/\d{4})\s+to\s+(\d{1,2}/\d{1,2}/\d{4})',
            r'(\w+\s+\d{1,2},\s+\d{4})\s+to\s+(\w+\s+\d{1,2},\s+\d{4})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    end_date_str = match.group(2)
                    # Try different date formats
                    for date_format in ['%m/%d/%Y', '%B %d, %Y']:
                        try:
                            end_date = datetime.strptime(end_date_str, date_format)
                            return end_date.strftime('%Y%m%d')
                        except ValueError:
                            continue
                except:
                    continue
        
        # Fallback: use current date
        return datetime.now().strftime('%Y%m%d')
    
    def parse_table_structure(self, text: str, source_date: str) -> List[Dict]:
        """Parse structured table data"""
        operations = []
        
        # Look for table headers to identify structure
        header_patterns = [
            r'OPERATION\s+DATE.*?OPERATION\s+TIME.*?OPERATION\s+TYPE.*?SECURITIES.*?SECURITY.*?OPERATION',
            r'Operation\s+Date.*?Operation\s+Time.*?Operation\s+Type.*?Securities.*?Security.*?Operation'
        ]
        
        table_start = None
        for pattern in header_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                table_start = match.end()
                break
        
        if table_start:
            # Extract table content after headers
            table_content = text[table_start:]
            operations = self.extract_operations_from_table(table_content, source_date)
        
        return operations
    
    def extract_operations_from_table(self, table_text: str, source_date: str) -> List[Dict]:
        """Extract operations from table content"""
        operations = []
        lines = table_text.split('\n')
        
        current_operation = {}
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Look for date pattern (start of new operation)
            date_match = re.search(r'^(\d{1,2}/\d{1,2}/\d{4})', line)
            if date_match:
                # Save previous operation if exists
                if current_operation.get('OperationDate'):
                    operations.append(self.complete_operation(current_operation, source_date))
                
                # Start new operation
                current_operation = {'OperationDate': date_match.group(1)}
                
                # Look for time on the same line
                time_match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M\s*[-–]\s*\d{1,2}:\d{2}\s*[AP]M)', line, re.IGNORECASE)
                if time_match:
                    current_operation['OperationTime'] = self.standardize_time_format(time_match.group(1))
                
                # Look for operation type
                operation_types = [
                    'TBA Purchase: 15-year Uniform MBS',
                    'TBA Purchase: 30-year Ginnie Mae',
                    'TBA Purchase',
                    'Purchase',
                    'Sale'
                ]
                
                for op_type in operation_types:
                    if op_type.lower() in line.lower():
                        current_operation['Operation Type'] = op_type
                        break
                
                continue
            
            # If we have a current operation, look for securities and amounts
            if current_operation.get('OperationDate'):
                # Look for securities (patterns like "FNCI 5.0", "G2SF 5.5")
                security_matches = re.findall(r'([A-Z]{2,}[A-Z0-9]*\s+\d+\.\d+)', line)
                
                # Look for amounts (patterns like "$24 Million", "26 Million")
                amount_matches = re.findall(r'\$?(\d+(?:\.\d+)?)\s*(Million|Billion)', line, re.IGNORECASE)
                
                if security_matches and amount_matches:
                    # Handle multiple securities in the same line
                    for i, security in enumerate(security_matches):
                        # Create separate operation for each security
                        op_copy = current_operation.copy()
                        op_copy['Securities Included (CUSP)'] = security
                        
                        # Assign amounts (individual security amount)
                        if i < len(amount_matches):
                            amount, unit = amount_matches[i]
                            op_copy['Security Maximums (Millions)'] = f"${amount} {unit.lower()}"
                        
                        # Find operation maximum (total amount)
                        total_amount = sum(float(amt[0]) for amt in amount_matches)
                        op_copy['OperationMaximum'] = f"${total_amount} {amount_matches[0][1].lower()}"
                        
                        operations.append(self.complete_operation(op_copy, source_date))
                        print(f"  Found operation: {op_copy['OperationDate']} {security} ${amount} {unit}")
                    
                    # Reset current_operation since we processed it
                    current_operation = {}
        
        # Handle last operation if exists
        if current_operation.get('OperationDate'):
            operations.append(self.complete_operation(current_operation, source_date))
        
        return operations
    
    def parse_line_by_line(self, text: str, source_date: str) -> List[Dict]:
        """Parse PDF line by line looking for operation data"""
        operations = []
        lines = text.split('\n')
        
        current_date = None
        current_time = None
        current_type = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Look for dates
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', line)
            if date_match:
                current_date = date_match.group(1)
            
            # Look for times
            time_match = re.search(r'(\d{1,2}:\d{2}\s*[AP]M\s*[-–]\s*\d{1,2}:\d{2}\s*[AP]M)', line, re.IGNORECASE)
            if time_match:
                current_time = self.standardize_time_format(time_match.group(1))
            
            # Look for operation types
            if 'TBA Purchase' in line:
                if '15-year' in line and 'Uniform' in line:
                    current_type = 'TBA Purchase: 15-year Uniform MBS'
                elif '30-year' in line and 'Ginnie' in line:
                    current_type = 'TBA Purchase: 30-year Ginnie Mae'
                else:
                    current_type = 'TBA Purchase'
            elif 'Purchase' in line:
                current_type = 'Purchase'
            elif 'Sale' in line:
                current_type = 'Sale'
            
            # Look for securities and amounts in the same line
            if current_date and line:
                securities = re.findall(r'([A-Z]{2,}[A-Z0-9]*\s+\d+\.\d+)', line)
                amounts = re.findall(r'\$?(\d+(?:\.\d+)?)\s*(Million|Billion)', line, re.IGNORECASE)
                
                if securities and amounts:
                    # Calculate total for operation maximum
                    total_amount = sum(float(amt[0]) for amt in amounts)
                    unit = amounts[0][1] if amounts else 'Million'
                    
                    for i, security in enumerate(securities):
                        # Individual amount for this security
                        if i < len(amounts):
                            amount, amount_unit = amounts[i]
                        else:
                            amount = amounts[0][0] if amounts else "0"
                            amount_unit = amounts[0][1] if amounts else 'Million'
                        
                        operation = {
                            'OperationDate': current_date,
                            'OperationTime': current_time or '',
                            'Operation Type': current_type or 'TBA Purchase',
                            'Settlement Date': '',
                            'Securities Included (CUSP)': security,
                            'Security Maximums (Millions)': f"${amount} {amount_unit.lower()}",
                            'OperationMaximum': f"${total_amount} {unit.lower()}",
                            'Source_Date': source_date
                        }
                        
                        operations.append(operation)
                        print(f"  Line parsing found: {current_date} {security} ${amount} {amount_unit}")
        
        return operations
    
    def parse_with_patterns(self, text: str, source_date: str) -> List[Dict]:
        """Parse using regex patterns to find operation data"""
        operations = []
        
        # Pattern to find complete operation information
        # This looks for: Date + Time + Type + Securities + Amounts
        operation_pattern = r'''
            (\d{1,2}/\d{1,2}/\d{4}).*?                      # Date
            (\d{1,2}:\d{2}\s*[AP]M\s*[-–]\s*\d{1,2}:\d{2}\s*[AP]M).*?  # Time
            (TBA\s+Purchase[^$]*?).*?                       # Operation type
            ([A-Z]{2,}[A-Z0-9]*\s+\d+\.\d+).*?            # Security
            \$?(\d+(?:\.\d+)?)\s*(Million|Billion)         # Amount
        '''
        
        matches = re.findall(operation_pattern, text, re.VERBOSE | re.IGNORECASE | re.DOTALL)
        
        for match in matches:
            date, time, op_type, security, amount, unit = match
            
            operation = {
                'OperationDate': date.strip(),
                'OperationTime': self.standardize_time_format(time.strip()),
                'Operation Type': op_type.strip(),
                'Settlement Date': '',
                'Securities Included (CUSP)': security.strip(),
                'Security Maximums (Millions)': f"${amount} {unit.lower()}",
                'OperationMaximum': f"${amount} {unit.lower()}",  # Will be updated if multiple securities found
                'Source_Date': source_date
            }
            
            operations.append(operation)
            print(f"  Pattern found: {date} {security} ${amount} {unit}")
        
        return operations
    
    def parse_fedtrade_pdf(self, text: str) -> List[Dict]:
        """Parse FedTrade Operations PDF"""
        operations = []
        
        print("Parsing FedTrade Operations PDF...")
        
        # Extract date range for source date
        source_date = self.extract_source_date(text)
        
        # Parse the table structure
        lines = text.split('\n')
        current_date = None
        current_time = None
        current_type = None
        date_operations = {}
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Check for date pattern
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', line)
            if date_match:
                current_date = date_match.group(1)
                continue
            
            # Check for time and operation type
            time_match = re.search(r'(\d{1,2}:\d{2}\s+[AP]M\s*-\s*\d{1,2}:\d{2}\s+[AP]M)', line)
            if time_match:
                current_time = self.standardize_time_format(time_match.group(1))
                
                # Extract operation type from the same line
                if '15-year' in line:
                    current_type = 'TBA Purchase: 15-year'
                elif '30-year' in line:
                    current_type = 'TBA Purchase: 30-year'
                else:
                    current_type = 'Purchase'
                
                continue
            
            # Check for securities and amounts
            if current_date and current_time and current_type:
                security_pattern = r'([A-Z]{2,}[A-Z0-9]*\s+\d+\.\d+)\s+\$(\d+)\s*(million|billion)'
                security_matches = re.findall(security_pattern, line, re.IGNORECASE)
                
                if security_matches:
                    date_time_key = (current_date, current_time)
                    if date_time_key not in date_operations:
                        date_operations[date_time_key] = {
                            'type': current_type,
                            'securities': [],
                            'total': 0
                        }
                    
                    for security, amount, unit in security_matches:
                        amount_value = float(amount)
                        date_operations[date_time_key]['securities'].append({
                            'security': security,
                            'amount': f'${amount} {unit.lower()}',
                            'amount_value': amount_value
                        })
                        date_operations[date_time_key]['total'] += amount_value
        
        # Convert grouped operations to final format
        for (op_date, op_time), op_data in date_operations.items():
            operation_max = f"${op_data['total']} million"
            
            for sec_data in op_data['securities']:
                operations.append({
                    'OperationDate': op_date,
                    'OperationTime': op_time,
                    'Operation Type': op_data['type'],
                    'Settlement Date': '',
                    'Securities Included (CUSP)': sec_data['security'],
                    'Security Maximums (Millions)': sec_data['amount'],
                    'OperationMaximum': operation_max,
                    'Source_Date': source_date
                })
        
        return operations
    
    def complete_operation(self, operation: Dict, source_date: str) -> Dict:
        """Complete operation with all required fields"""
        return {
            'OperationDate': operation.get('OperationDate', ''),
            'OperationTime': operation.get('OperationTime', ''),
            'Operation Type': operation.get('Operation Type', ''),
            'Settlement Date': operation.get('Settlement Date', ''),
            'Securities Included (CUSP)': operation.get('Securities Included (CUSP)', ''),
            'Security Maximums (Millions)': operation.get('Security Maximums (Millions)', ''),
            'OperationMaximum': operation.get('OperationMaximum', ''),
            'Source_Date': source_date
        }
    
    def standardize_time_format(self, time_str: str) -> str:
        """Convert time format to match expected output"""
        # Clean up the time string
        time_str = re.sub(r'\s+', '', time_str)  # Remove extra spaces
        time_str = time_str.replace('–', '-').replace('—', '-')  # Normalize dashes
        
        # Ensure AM/PM is properly formatted
        time_str = re.sub(r'([AP])\.?M\.?', r'\1M', time_str, flags=re.IGNORECASE)
        
        return time_str
    
    def parse_pdf(self, pdf_path: str) -> List[Dict]:
        """Main parsing function"""
        print(f"Parsing PDF: {pdf_path}")
        
        text = self.extract_text_from_pdf(pdf_path)
        if not text:
            print("Error: Could not extract text from PDF")
            return []
        
        pdf_type = self.detect_pdf_type(text)
        print(f"Detected PDF type: {pdf_type}")
        
        if pdf_type == "small_value":
            operations = self.parse_small_value_pdf(text)
        else:
            operations = self.parse_fedtrade_pdf(text)
        
        print(f"Parsed {len(operations)} operations")
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
    parser = argparse.ArgumentParser(description='Parse FEDAO PDF files to CSV format')
    parser.add_argument('input_pdf', nargs='?', help='Input PDF file path')
    parser.add_argument('output_csv', nargs='?', default='FEDAO_MOA_DATA.csv', 
                       help='Output CSV file path')
    parser.add_argument('--append', action='store_true', 
                       help='Append to existing CSV file')
    parser.add_argument('--directory', action='store_true',
                       help='Process all PDF files in directory')
    
    args = parser.parse_args()
    
    fedao_parser = FEDAOParser(process_directory=args.directory)
    
    if args.directory:
        print("Processing all PDF files in the script directory...")
        output_path = args.output_csv
        if not os.path.isabs(output_path):
            output_path = os.path.join(fedao_parser.script_dir, output_path)
        
        processed_count = fedao_parser.process_directory_pdfs(output_path, append=args.append)
        
        if processed_count == 0:
            print("No PDF files were successfully processed")
            sys.exit(1)
            
    elif args.input_pdf:
        operations = fedao_parser.parse_pdf(args.input_pdf)
        
        if operations:
            output_path = args.output_csv
            if not os.path.isabs(output_path):
                output_path = os.path.join(fedao_parser.script_dir, output_path)
            
            fedao_parser.save_to_csv(operations, output_path, append=args.append)
            
            print(f"\nSummary:")
            print(f"- Input PDF: {args.input_pdf}")
            print(f"- Output CSV: {output_path}")
            print(f"- Operations parsed: {len(operations)}")
            print(f"- Mode: {'Append' if args.append else 'Overwrite'}")
            
            if operations:
                print(f"\nOperations found:")
                for op in operations:
                    print(f"  {op['OperationDate']}: {op['Securities Included (CUSP)']} {op['Security Maximums (Millions)']}")
        else:
            print("No operations were parsed from the PDF")
            sys.exit(1)
    else:
        print("No input PDF specified. Processing all PDF files in the script directory...")
        output_path = args.output_csv
        if not os.path.isabs(output_path):
            output_path = os.path.join(fedao_parser.script_dir, output_path)
        
        processed_count = fedao_parser.process_directory_pdfs(output_path, append=args.append)
        
        if processed_count == 0:
            print("\nNo PDF files found or processed successfully.")
            print("Usage options:")
            print("  python fedao_parser.py input.pdf [output.csv]")
            print("  python fedao_parser.py --directory [output.csv]")
            sys.exit(1)

if __name__ == "__main__":
    main()