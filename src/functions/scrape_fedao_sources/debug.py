#!/usr/bin/env python3
"""
Trace exactly what happens to the release date variable
"""

import os
import re
import time
import csv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

def setup_driver():
    """Setup Chrome driver"""
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    return webdriver.Chrome(options=chrome_options)

def extract_release_date(driver):
    """Extract release date and trace every step"""
    print("\n" + "="*60)
    print("TRACING RELEASE DATE EXTRACTION")
    print("="*60)
    
    extracted_release_date = None
    
    try:
        print("Step 1: Looking for monthly-details element...")
        element = driver.find_element(By.ID, "monthly-details")
        print("✅ Found monthly-details element")
        
        element_text = element.text.strip()
        print(f"Step 2: Element text (first 300 chars): '{element_text[:300]}...'")
        
        print("Step 3: Searching for date pattern...")
        period_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})', element_text)
        
        if period_match:
            start_date_str = period_match.group(1)
            end_date_str = period_match.group(2)
            print(f"✅ Found date pattern: {start_date_str} - {end_date_str}")
            
            print("Step 4: Converting end date to release date format...")
            end_date_obj = datetime.strptime(end_date_str, '%m/%d/%Y')
            extracted_release_date = str(int(end_date_obj.strftime('%Y%m%d')))
            print(f"✅ Converted: {end_date_str} -> {extracted_release_date}")
            
        else:
            print("❌ No date pattern found")
            
    except Exception as e:
        print(f"❌ Error in extraction: {e}")
    
    print(f"\nFINAL RESULT: extracted_release_date = {extracted_release_date}")
    print("="*60)
    
    return extracted_release_date

def simulate_operation_processing(extracted_release_date):
    """Simulate the operation processing to see where release date gets lost"""
    print("\n" + "="*60)
    print("TRACING OPERATION PROCESSING")
    print("="*60)
    
    # Simulate operations data (like what comes from Current Schedule tab)
    operations = [
        {
            'OPERATION DATE': '6/24/2025',
            'OPERATION TIME (ET)': '10:10 - 10:30 AM',
            'SETTLEMENT DATE': '6/25/2025',
            'OPERATION TYPE': 'Small Value Purchase',
            'SECURITY TYPE AND MATURITY': 'Nominal 10 to 22.5',
            'MATURITY RANGE': '6/25/2035 - 12/24/2047',
            'MAXIMUM OPERATION CURRENCY': '$',
            'MAXIMUM OPERATION SIZE': '50',
            'MAXIMUM OPERATION MULTIPLIER': 'million',
            'release_date': ''  # Initially empty
        },
        {
            'OPERATION DATE': '6/26/2025',
            'OPERATION TIME (ET)': '10:10 - 10:30 AM', 
            'SETTLEMENT DATE': '6/27/2025',
            'OPERATION TYPE': 'Small Value Sale',
            'SECURITY TYPE AND MATURITY': 'TIPS 7.5 to 30.0',
            'MATURITY RANGE': '12/27/2032 - 6/27/2055',
            'MAXIMUM OPERATION CURRENCY': '$',
            'MAXIMUM OPERATION SIZE': '25',
            'MAXIMUM OPERATION MULTIPLIER': 'million',
            'release_date': ''  # Initially empty
        }
    ]
    
    print(f"Step 1: Processing {len(operations)} operations")
    print(f"Step 2: extracted_release_date = {extracted_release_date}")
    
    # Simulate the assignment logic from the main code
    if operations and extracted_release_date:
        print("Step 3: Both operations and extracted_release_date exist - applying...")
        for i, operation in enumerate(operations):
            print(f"  Before assignment - Operation {i+1} release_date: '{operation['release_date']}'")
            operation['release_date'] = int(extracted_release_date)
            print(f"  After assignment - Operation {i+1} release_date: '{operation['release_date']}'")
    elif operations:
        print("❌ Operations exist but extracted_release_date is None/empty - using fallback")
        for i, operation in enumerate(operations):
            operation['release_date'] = int(datetime.now().strftime('%Y%m%d'))
            print(f"  Fallback - Operation {i+1} release_date: '{operation['release_date']}'")
    else:
        print("❌ No operations to process")
    
    print(f"\nStep 4: Final operations data:")
    for i, op in enumerate(operations):
        print(f"  Operation {i+1}: release_date = {op['release_date']}")
    
    return operations

def test_csv_output(operations):
    """Test the CSV output to see if release_date survives"""
    print("\n" + "="*60)
    print("TRACING CSV OUTPUT")
    print("="*60)
    
    # Simulate standardization (like in the main code)
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
        print(f"Standardized operation: release_date = {standardized_op['release_date']}")
    
    # Test CSV writing
    filename = "test_release_date.csv"
    fieldnames = [
        'operation_date', 'operation_time', 'settlement_date',
        'operation_type', 'security_type_and_maturity', 'maturity_range',
        'maximum_operation_currency', 'maximum_operation_size', 
        'maximum_operation_multiplier', 'release_date'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for operation in standardized:
            row = {}
            for field in fieldnames:
                value = operation.get(field, '')
                # Clean up formatting
                if isinstance(value, str):
                    value = value.replace(',', '').strip()
                row[field] = value
                if field == 'release_date':
                    print(f"Writing release_date to CSV: '{value}'")
            writer.writerow(row)
    
    print(f"✅ CSV written to {filename}")
    
    # Read back and verify
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
        print(f"\nCSV content:\n{content}")

def main():
    """Main test function"""
    url = "https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details"
    
    driver = setup_driver()
    
    try:
        print(f"Loading URL: {url}")
        driver.get(url)
        time.sleep(10)  # Wait for page to load
        
        # Step 1: Extract release date
        extracted_release_date = extract_release_date(driver)
        
        # Step 2: Process operations (simulated)
        operations = simulate_operation_processing(extracted_release_date)
        
        # Step 3: Test CSV output
        test_csv_output(operations)
        
        print("\n" + "="*60)
        print("DIAGNOSIS COMPLETE")
        print("="*60)
        print("Check the output above to see where the release_date gets lost!")
        
    except Exception as e:
        print(f"Critical error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        input("Press Enter to close browser...")
        driver.quit()

if __name__ == "__main__":
    main()