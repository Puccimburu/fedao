#!/usr/bin/env python3
"""
FEDAO Cloud Function - REAL DATA FOR BOTH TOA AND MOA
- TOA: Real FRBNY scraper (Treasury Securities) 
- MOA: Real PDF download + parser (Mortgage-Backed Securities)
- FIXED: Field name matching for MOA processing
"""

import os
import json
import base64
import logging
import tempfile
from datetime import datetime
from typing import Dict, Any
from google.cloud import storage
import functions_framework

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to import the parsers safely
FRBNY_SCRAPER_AVAILABLE = False
FEDAO_PARSER_AVAILABLE = False

try:
    from frbny_parser import CombinedFRBNYScraper
    FRBNY_SCRAPER_AVAILABLE = True
    logger.info("✅ Successfully imported CombinedFRBNYScraper")
except ImportError as e:
    logger.error(f"❌ Failed to import CombinedFRBNYScraper: {e}")
except Exception as e:
    logger.error(f"❌ Error importing CombinedFRBNYScraper: {e}")

try:
    from fedao_parser import FEDAOParser
    FEDAO_PARSER_AVAILABLE = True
    logger.info("✅ Successfully imported FEDAOParser")
except ImportError as e:
    logger.error(f"❌ Failed to import FEDAOParser: {e}")
except Exception as e:
    logger.error(f"❌ Error importing FEDAOParser: {e}")

@functions_framework.cloud_event
def fedao_scraper_main(cloud_event):
    """
    REAL FEDAO Scraper - Both TOA and MOA with REAL current data
    """
    try:
        # Parse trigger message
        if hasattr(cloud_event, 'data') and cloud_event.data:
            if 'message' in cloud_event.data:
                message_data = cloud_event.data['message']
                if 'data' in message_data:
                    decoded_data = base64.b64decode(message_data['data']).decode('utf-8')
                    trigger_data = json.loads(decoded_data)
                else:
                    trigger_data = message_data
            else:
                trigger_data = cloud_event.data
        else:
            trigger_data = {"mode": "both"}
        
        mode = trigger_data.get('mode', 'both')
        
        # Generate timestamp for this processing run
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        logger.info(f"🚀 REAL FEDAO SCRAPER - Mode: {mode}, Timestamp: {timestamp}")
        logger.info(f"📊 Parser availability - FRBNY: {FRBNY_SCRAPER_AVAILABLE}, FEDAO: {FEDAO_PARSER_AVAILABLE}")
        
        # Get environment variables
        project_id = os.environ.get('GCP_PROJECT')
        bucket_name = os.environ.get('FEDAO_OUTPUT_BUCKET')
        
        if not project_id or not bucket_name:
            raise ValueError("Missing required environment variables")
        
        # Initialize storage client
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        
        results = {"mode": mode, "files_created": [], "errors": [], "timestamp": timestamp}
        
        # Process TOA data with REAL FRBNY SCRAPER
        if mode in ["both", "toa"]:
            if FRBNY_SCRAPER_AVAILABLE:
                try:
                    logger.info("📊 Processing TOA with REAL FRBNY scraper...")
                    toa_data = process_toa_with_real_scraper()
                    
                    if toa_data:
                        # Create timestamped filename for TOA
                        toa_filename = f"FEDAO_TOA_DATA_{timestamp}.csv"
                        toa_path = f"FRBNY/FEDAO/{toa_filename}"
                        
                        upload_csv_data(bucket, toa_data, toa_path)
                        results["files_created"].append(f"gs://{bucket_name}/{toa_path}")
                        logger.info(f"✅ REAL TOA processing completed: {toa_filename}")
                        
                        # Also save as the latest version
                        latest_toa_path = "FRBNY/FEDAO/FEDAO_TOA_DATA_LATEST.csv"
                        upload_csv_data(bucket, toa_data, latest_toa_path)
                        results["files_created"].append(f"gs://{bucket_name}/{latest_toa_path}")
                    else:
                        raise Exception("No data extracted from FRBNY Treasury website")
                    
                except Exception as e:
                    logger.error(f"❌ TOA processing failed: {e}")
                    results["errors"].append(f"TOA: {str(e)}")
            else:
                logger.warning("⚠️  FRBNY scraper not available")
                results["errors"].append("TOA: FRBNY scraper import failed")
        
        # Process MOA data with REAL PDF DOWNLOAD + PARSER
        if mode in ["both", "moa"]:
            if FEDAO_PARSER_AVAILABLE:
                try:
                    logger.info("📄 Processing MOA with REAL PDF download + parser...")
                    moa_data = process_moa_with_real_pdf_download()
                    
                    if moa_data:
                        # Create timestamped filename for MOA
                        moa_filename = f"FEDAO_MOA_DATA_{timestamp}.csv"
                        moa_path = f"FRBNY/FEDAO/{moa_filename}"
                        
                        upload_csv_data(bucket, moa_data, moa_path)
                        results["files_created"].append(f"gs://{bucket_name}/{moa_path}")
                        logger.info(f"✅ REAL MOA processing completed: {moa_filename}")
                        
                        # Also save as the latest version
                        latest_moa_path = "FRBNY/FEDAO/FEDAO_MOA_DATA_LATEST.csv"
                        upload_csv_data(bucket, moa_data, latest_moa_path)
                        results["files_created"].append(f"gs://{bucket_name}/{latest_moa_path}")
                    else:
                        logger.warning("⚠️  No MOA data extracted")
                        
                except Exception as e:
                    logger.error(f"❌ MOA processing failed: {e}")
                    results["errors"].append(f"MOA: {str(e)}")
            else:
                logger.warning("⚠️  FEDAO parser not available")
                results["errors"].append("MOA: FEDAO parser import failed")
        
        # Create a processing summary file
        summary_data = create_processing_summary(mode, timestamp, results)
        summary_filename = f"PROCESSING_SUMMARY_{timestamp}.json"
        summary_path = f"FRBNY/FEDAO/summaries/{summary_filename}"
        upload_json_data(bucket, summary_data, summary_path)
        
        return {
            "status": "success" if not results["errors"] else "partial",
            "mode": mode,
            "timestamp": timestamp,
            "files_created": results["files_created"],
            "errors": results["errors"],
            "parsers_available": {
                "frbny_scraper": FRBNY_SCRAPER_AVAILABLE,
                "fedao_parser": FEDAO_PARSER_AVAILABLE
            },
            "message": f"🎉 REAL FEDAO processing completed at {timestamp}"
        }
        
    except Exception as e:
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        logger.error(f"💥 Cloud Function failed at {timestamp}: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "status": "error",
            "timestamp": timestamp,
            "message": str(e),
            "errors": [str(e)]
        }

def process_toa_with_real_scraper():
    """Process TOA using REAL FRBNY scraper"""
    logger.info("🌐 Starting REAL FRBNY Treasury Securities scraping...")
    
    try:
        # Initialize the real scraper
        scraper = CombinedFRBNYScraper()
        
        # FRBNY Treasury Securities URL
        url = "https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details"
        
        # Run the scraper
        success = scraper.run(url)
        
        if success and scraper.data:
            logger.info(f"✅ Successfully scraped {len(scraper.data)} Treasury operations from FRBNY")
            logger.info(f"📊 Data source: {scraper.source_type}")
            
            # Convert to standardized CSV format
            standardized_data = scraper.standardize_output_format(scraper.data)
            
            # Convert to CSV string
            import csv
            import io
            
            fieldnames = [
                'operation_date', 'operation_time', 'settlement_date', 'operation_type',
                'security_type_and_maturity', 'maturity_range', 'maximum_operation_currency',
                'maximum_operation_size', 'maximum_operation_multiplier', 'release_date'
            ]
            
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
            writer.writeheader()
            
            for operation in standardized_data:
                row = {}
                for field in fieldnames:
                    # Clean up HTML tags (especially <br> tags) from the data
                    value = str(operation.get(field, ''))
                    value = value.replace('<br>', ' ')
                    value = value.replace('<BR>', ' ')
                    value = value.replace('&nbsp;', ' ')
                    # Clean up multiple spaces
                    import re
                    value = re.sub(r'\s+', ' ', value).strip()
                    row[field] = value
                writer.writerow(row)
            
            csv_content = csv_buffer.getvalue()
            
            # Log sample of real data
            if standardized_data:
                sample = standardized_data[0]
                logger.info(f"📋 Sample Treasury operation: {sample.get('operation_date')} - {sample.get('operation_type')} - {sample.get('maximum_operation_currency')}{sample.get('maximum_operation_size')} {sample.get('maximum_operation_multiplier')}")
            
            return csv_content
            
        else:
            logger.error("❌ Treasury scraper failed or returned no data")
            return None
            
    except Exception as e:
        logger.error(f"❌ Real Treasury scraper failed: {e}")
        raise e

def process_moa_with_real_pdf_download():
    """REAL MOA processing - FIXED to match your exact FEDAOParser"""
    logger.info("🌐 Starting REAL MOA processing - CURRENT SCHEDULE PDF ONLY")
    
    try:
        import requests
        import tempfile
        import os
        import csv
        import io
        
        # Initialize YOUR FEDAOParser exactly as it is
        fedao_parser = FEDAOParser()
        
        # First, get the current schedule page to find the actual PDF URL
        schedule_page_url = "https://www.newyorkfed.org/markets/ambs_operation_schedule"
        
        logger.info(f"🔍 Getting current schedule page: {schedule_page_url}")
        
        page_response = requests.get(schedule_page_url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        page_response.raise_for_status()
        
        # Extract the current schedule PDF URL from the page
        import re
        from urllib.parse import urljoin
        
        # Look for the PDF link in the current schedule section
        pdf_pattern = r'href="([^"]*AMBS-Schedule[^"]*\.pdf[^"]*)"'
        pdf_matches = re.findall(pdf_pattern, page_response.text, re.IGNORECASE)
        
        if not pdf_matches:
            # Fallback: look for any schedule PDF
            pdf_pattern = r'href="([^"]*Schedule[^"]*\.pdf[^"]*)"'
            pdf_matches = re.findall(pdf_pattern, page_response.text, re.IGNORECASE)
        
        if not pdf_matches:
            raise Exception("No current schedule PDF found on FRBNY page")
        
        # Use the first (most current) PDF found
        pdf_relative_url = pdf_matches[0]
        current_schedule_pdf = urljoin(schedule_page_url, pdf_relative_url)
        
        logger.info(f"✅ Found current schedule PDF: {current_schedule_pdf}")
        logger.info(f"📥 Downloading current schedule PDF...")
        
        # Download the current schedule PDF
        pdf_response = requests.get(current_schedule_pdf, timeout=60, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        pdf_response.raise_for_status()
        
        # Validate PDF content
        content_type = pdf_response.headers.get('content-type', '').lower()
        if 'pdf' not in content_type:
            raise Exception(f"Expected PDF, got {content_type}")
        
        if len(pdf_response.content) < 1000:
            raise Exception(f"PDF too small: {len(pdf_response.content)} bytes")
        
        logger.info(f"✅ Downloaded PDF successfully: {len(pdf_response.content)} bytes")
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file.write(pdf_response.content)
            temp_file_path = temp_file.name
        
        logger.info(f"💾 Saved PDF to: {temp_file_path}")
        
        # Parse the PDF using YOUR EXACT FEDAOParser
        try:
            logger.info("📄 Parsing PDF with YOUR FEDAOParser...")
            
            # Call YOUR parse_pdf method exactly as it is
            operations = fedao_parser.parse_pdf(temp_file_path)
            
            if operations:
                logger.info(f"🎉 SUCCESS: Extracted {len(operations)} REAL operations from current schedule")
                
                # Debug: Show what YOUR parser actually returns
                logger.info("📋 Debugging YOUR parser's output:")
                if operations:
                    sample_op = operations[0]
                    logger.info(f"Sample operation type: {type(sample_op)}")
                    logger.info(f"Sample operation keys: {list(sample_op.keys())}")
                    logger.info(f"Sample operation: {sample_op}")
                
                # Log each operation YOUR parser found
                for i, op in enumerate(operations):
                    logger.info(f"   📋 Real Operation {i+1}:")
                    logger.info(f"       OperationDate: {op.get('OperationDate', 'MISSING')}")
                    logger.info(f"       OperationTime: {op.get('OperationTime', 'MISSING')}")
                    logger.info(f"       Operation Type: {op.get('Operation Type', 'MISSING')}")
                    logger.info(f"       Securities Included (CUSP): {op.get('Securities Included (CUSP)', 'MISSING')}")
                    logger.info(f"       Security Maximums (Millions): {op.get('Security Maximums (Millions)', 'MISSING')}")
                    logger.info(f"       OperationMaximum: {op.get('OperationMaximum', 'MISSING')}")
                    logger.info(f"       Source_Date: {op.get('Source_Date', 'MISSING')}")
                
                # Use YOUR parser's exact csv_columns
                fieldnames = fedao_parser.csv_columns
                logger.info(f"Using YOUR parser's fieldnames: {fieldnames}")
                
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
                writer.writeheader()
                
                for operation in operations:
                    # Create row exactly as YOUR parser expects
                    row = {}
                    for field in fieldnames:
                        value = operation.get(field, '')
                        row[field] = value
                        if not value:  # Log missing fields
                            logger.warning(f"Missing field '{field}' in operation: {operation}")
                    
                    logger.info(f"Writing CSV row: {row}")
                    writer.writerow(row)
                
                csv_content = csv_buffer.getvalue()
                
                # Debug: Show the actual CSV content
                logger.info(f"📊 Generated CSV content:")
                lines = csv_content.split('\n')
                for i, line in enumerate(lines[:10]):  # Show first 10 lines
                    logger.info(f"CSV line {i}: {line}")
                
                if len(csv_content.strip()) > len(','.join(fieldnames)):  # More than just header
                    logger.info(f"📊 SUCCESS: REAL MOA CSV created with {len(operations)} operations")
                    return csv_content
                else:
                    logger.error("❌ CSV only contains headers - no data rows!")
                    return create_diagnostic_csv("CSV generated but contains no data rows")
                
            else:
                logger.warning("⚠️  YOUR parser returned empty operations list")
                
                # Debug: Try to extract text to see what's in the PDF
                text = fedao_parser.extract_text_from_pdf(temp_file_path)
                if text:
                    logger.info(f"📝 PDF text preview (first 1000 chars):")
                    logger.info(f"{text[:1000]}")
                    pdf_type = fedao_parser.detect_pdf_type(text)
                    logger.info(f"🔍 YOUR parser detected PDF type: {pdf_type}")
                    
                    # Try to understand why parsing failed
                    logger.info("🔍 Checking PDF content for expected patterns:")
                    if "6/3/2025" in text:
                        logger.info("  ✓ Found date pattern 6/3/2025")
                    if "FNCI" in text:
                        logger.info("  ✓ Found FNCI security")
                    if "G2SF" in text:
                        logger.info("  ✓ Found G2SF security")
                    if "million" in text.lower():
                        logger.info("  ✓ Found million keyword")
                    if "TBA Purchase" in text:
                        logger.info("  ✓ Found TBA Purchase")
                else:
                    logger.warning("❌ Could not extract any text from PDF")
                
                return create_diagnostic_csv("YOUR parser returned no operations")
        
        except Exception as parse_error:
            logger.error(f"❌ Error with YOUR parser: {parse_error}")
            import traceback
            logger.error(f"YOUR parser error traceback: {traceback.format_exc()}")
            return create_diagnostic_csv(f"YOUR parser error: {str(parse_error)}")
        
        finally:
            # Clean up temp file
            try:
                os.unlink(temp_file_path)
                logger.info("🧹 Cleaned up temporary PDF file")
            except:
                pass
            
    except Exception as e:
        logger.error(f"❌ REAL MOA PDF download/processing failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return create_diagnostic_csv(f"Download/processing error: {str(e)}")

def create_diagnostic_csv(reason):
    """Create CSV with diagnostic info when real parsing fails - NO HARDCODED DATA"""
    import csv
    import io
    from datetime import datetime
    
    logger.info(f"📋 Creating diagnostic CSV: {reason}")
    
    # FIXED: Use FEDAOParser's actual column names
    fieldnames = [
        'OperationDate', 'OperationTime', 'Operation Type', 'Settlement Date',
        'Securities Included (CUSP)', 'Security Maximums (Millions)', 
        'OperationMaximum', 'Source_Date'
    ]
    
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    
    # Add a diagnostic row explaining what happened
    writer.writerow({
        'OperationDate': 'DIAGNOSTIC',
        'OperationTime': reason,
        'Operation Type': 'REAL_DATA_ONLY',
        'Settlement Date': '',
        'Securities Included (CUSP)': 'Check current FRBNY schedule PDF manually',
        'Security Maximums (Millions)': 'No fallback data',
        'OperationMaximum': '$0',
        'Source_Date': datetime.now().strftime('%Y%m%d')
    })
    
    return csv_buffer.getvalue()

def create_processing_summary(mode: str, timestamp: str, results: dict) -> str:
    """Create a processing summary in JSON format"""
    import json
    
    summary = {
        "processing_timestamp": timestamp,
        "processing_mode": mode,
        "status": "success" if not results.get("errors") else "partial",
        "files_created": results.get("files_created", []),
        "errors": results.get("errors", []),
        "total_files": len(results.get("files_created", [])),
        "moa_processed": mode in ["both", "moa"],
        "toa_processed": mode in ["both", "toa"],
        "utc_timestamp": datetime.utcnow().isoformat(),
        "parsers_available": {
            "frbny_scraper": FRBNY_SCRAPER_AVAILABLE,
            "fedao_parser": FEDAO_PARSER_AVAILABLE
        },
        "processing_info": {
            "function_name": "scrape-fedao-sources",
            "version": "real_data_v1_fixed",
            "environment": "cloud_function_gen2",
            "features": ["real_toa_scraper", "real_moa_pdf_parser", "fixed_field_mapping"]
        }
    }
    
    return json.dumps(summary, indent=2)

def upload_csv_data(bucket, csv_content, blob_path):
    """Upload CSV data to GCS"""
    blob = bucket.blob(blob_path)
    blob.upload_from_string(csv_content, content_type='text/csv')
    logger.info(f"📤 Uploaded CSV: {blob_path}")

def upload_json_data(bucket, json_content, blob_path):
    """Upload JSON data to GCS"""
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json_content, content_type='application/json')
    logger.info(f"📤 Uploaded summary: {blob_path}")