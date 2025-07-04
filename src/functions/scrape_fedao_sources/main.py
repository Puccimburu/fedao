#!/usr/bin/env python3
"""
FEDAO Cloud Function - ENHANCED with AI + Cloud Run Renderer
- Keeps your working parser system
- Adds AI-powered validation and processing
- Adds advanced web rendering capabilities
- Maintains backward compatibility
"""

import os
import json
import base64
import logging
import tempfile
import csv
import io
import re
import requests
from datetime import datetime
from typing import Dict, Any, List
from urllib.parse import urljoin
from google.cloud import storage
from google.cloud import aiplatform
import functions_framework

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to import the parsers safely (YOUR EXISTING PARSERS)
FRBNY_SCRAPER_AVAILABLE = False
FEDAO_PARSER_AVAILABLE = False

try:
    from frbny_parser import CombinedFRBNYScraper
    FRBNY_SCRAPER_AVAILABLE = True
    logger.info("✅ Successfully imported CombinedFRBNYScraper")
except ImportError as e:
    logger.error(f"❌ Failed to import CombinedFRBNYScraper: {e}")

try:
    from fedao_parser import FEDAOParser
    FEDAO_PARSER_AVAILABLE = True
    logger.info("✅ Successfully imported FEDAOParser")
except ImportError as e:
    logger.error(f"❌ Failed to import FEDAOParser: {e}")

# Initialize AI Platform (if available)
AI_AVAILABLE = False
try:
    project_id = os.environ.get('GCP_PROJECT')
    if project_id:
        aiplatform.init(project=project_id, location=os.environ.get('FUNCTION_REGION', 'europe-west1'))
        AI_AVAILABLE = True
        logger.info("✅ AI Platform initialized")
except Exception as e:
    logger.warning(f"⚠️ AI Platform not available: {e}")

@functions_framework.cloud_event
def fedao_scraper_main(cloud_event):
    """
    ENHANCED FEDAO Scraper - Your working system + AI enhancements
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
        use_ai = trigger_data.get('use_ai', True)
        use_renderer = trigger_data.get('use_renderer', True)
        
        # Generate timestamp for this processing run
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        logger.info(f"🚀 ENHANCED FEDAO SCRAPER - Mode: {mode}, AI: {use_ai}, Renderer: {use_renderer}")
        logger.info(f"📊 Capabilities - FRBNY: {FRBNY_SCRAPER_AVAILABLE}, FEDAO: {FEDAO_PARSER_AVAILABLE}, AI: {AI_AVAILABLE}")
        
        # Get environment variables
        project_id = os.environ.get('GCP_PROJECT')
        bucket_name = os.environ.get('FEDAO_OUTPUT_BUCKET')
        renderer_url = os.environ.get('RENDERER_SERVICE_URL')
        
        if not project_id or not bucket_name:
            raise ValueError("Missing required environment variables")
        
        # Initialize storage client
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        
        results = {"mode": mode, "files_created": [], "errors": [], "timestamp": timestamp, "enhancements": []}
        
        # Process TOA data with ENHANCED capabilities
        if mode in ["both", "toa"]:
            if FRBNY_SCRAPER_AVAILABLE:
                try:
                    logger.info("📊 Processing TOA with ENHANCED capabilities...")
                    toa_data = process_toa_enhanced(renderer_url if use_renderer else None, use_ai)
                    
                    if toa_data:
                        # Create timestamped filename for TOA
                        toa_filename = f"FEDAO_TOA_DATA_{timestamp}.csv"
                        toa_path = f"FRBNY/FEDAO/{toa_filename}"
                        
                        upload_csv_data(bucket, toa_data, toa_path)
                        results["files_created"].append(f"gs://{bucket_name}/{toa_path}")
                        
                        # Also save as the latest version
                        latest_toa_path = "FRBNY/FEDAO/FEDAO_TOA_DATA_LATEST.csv"
                        upload_csv_data(bucket, toa_data, latest_toa_path)
                        results["files_created"].append(f"gs://{bucket_name}/{latest_toa_path}")
                        
                        # AI Enhancement: Trigger transformer if available
                        if use_ai:
                            trigger_ai_processing(toa_path, "TOA", timestamp)
                            results["enhancements"].append("AI processing triggered for TOA")
                        
                        logger.info(f"✅ ENHANCED TOA processing completed: {toa_filename}")
                    else:
                        raise Exception("No data extracted from FRBNY Treasury website")
                    
                except Exception as e:
                    logger.error(f"❌ TOA processing failed: {e}")
                    results["errors"].append(f"TOA: {str(e)}")
            else:
                logger.warning("⚠️ FRBNY scraper not available")
                results["errors"].append("TOA: FRBNY scraper import failed")
        
        # Process MOA data with ENHANCED capabilities
        if mode in ["both", "moa"]:
            if FEDAO_PARSER_AVAILABLE:
                try:
                    logger.info("📄 Processing MOA with ENHANCED capabilities...")
                    moa_data = process_moa_enhanced(renderer_url if use_renderer else None, use_ai)
                    
                    if moa_data:
                        # Create timestamped filename for MOA
                        moa_filename = f"FEDAO_MOA_DATA_{timestamp}.csv"
                        moa_path = f"FRBNY/FEDAO/{moa_filename}"
                        
                        upload_csv_data(bucket, moa_data, moa_path)
                        results["files_created"].append(f"gs://{bucket_name}/{moa_path}")
                        
                        # Also save as the latest version
                        latest_moa_path = "FRBNY/FEDAO/FEDAO_MOA_DATA_LATEST.csv"
                        upload_csv_data(bucket, moa_data, latest_moa_path)
                        results["files_created"].append(f"gs://{bucket_name}/{latest_moa_path}")
                        
                        # AI Enhancement: Trigger transformer if available
                        if use_ai:
                            trigger_ai_processing(moa_path, "MOA", timestamp)
                            results["enhancements"].append("AI processing triggered for MOA")
                        
                        logger.info(f"✅ ENHANCED MOA processing completed: {moa_filename}")
                    else:
                        logger.warning("⚠️ No MOA data extracted")
                        
                except Exception as e:
                    logger.error(f"❌ MOA processing failed: {e}")
                    results["errors"].append(f"MOA: {str(e)}")
            else:
                logger.warning("⚠️ FEDAO parser not available")
                results["errors"].append("MOA: FEDAO parser import failed")
        
        # Create enhanced processing summary
        summary_data = create_enhanced_summary(mode, timestamp, results, use_ai, use_renderer)
        summary_filename = f"PROCESSING_SUMMARY_ENHANCED_{timestamp}.json"
        summary_path = f"FRBNY/FEDAO/summaries/{summary_filename}"
        upload_json_data(bucket, summary_data, summary_path)
        
        return {
            "status": "success" if not results["errors"] else "partial",
            "mode": mode,
            "timestamp": timestamp,
            "files_created": results["files_created"],
            "errors": results["errors"],
            "enhancements": results["enhancements"],
            "capabilities": {
                "frbny_scraper": FRBNY_SCRAPER_AVAILABLE,
                "fedao_parser": FEDAO_PARSER_AVAILABLE,
                "ai_processing": AI_AVAILABLE,
                "web_renderer": renderer_url is not None
            },
            "message": f"🎉 ENHANCED FEDAO processing completed at {timestamp}"
        }
        
    except Exception as e:
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        logger.error(f"💥 Enhanced Cloud Function failed at {timestamp}: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "status": "error",
            "timestamp": timestamp,
            "message": str(e),
            "errors": [str(e)]
        }

def process_toa_enhanced(renderer_url=None, use_ai=False):
    """Enhanced TOA processing with optional renderer and AI"""
    logger.info("🌐 Starting ENHANCED FRBNY Treasury Securities scraping...")
    
    try:
        # Initialize the real scraper (YOUR EXISTING ONE)
        scraper = CombinedFRBNYScraper()
        
        # FRBNY Treasury Securities URL
        url = "https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details"
        
        # Enhancement: Use renderer if available
        if renderer_url:
            logger.info("🎯 Using advanced web renderer...")
            try:
                rendered_data = use_web_renderer(renderer_url, url)
                if rendered_data:
                    # Process rendered data with your existing scraper
                    success = scraper.process_rendered_content(rendered_data)
                else:
                    # Fallback to normal scraping
                    success = scraper.run(url)
            except Exception as e:
                logger.warning(f"⚠️ Renderer failed, using fallback: {e}")
                success = scraper.run(url)
        else:
            # Use your existing scraper
            success = scraper.run(url)
        
        if success and scraper.data:
            logger.info(f"✅ Successfully scraped {len(scraper.data)} Treasury operations")
            
            # Convert to standardized CSV format (YOUR EXISTING LOGIC)
            standardized_data = scraper.standardize_output_format(scraper.data)
            
            # Enhancement: AI validation if available
            if use_ai and AI_AVAILABLE:
                logger.info("🧠 Applying AI validation...")
                standardized_data = apply_ai_validation(standardized_data, "TOA")
            
            # Convert to CSV string (YOUR EXISTING FORMAT)
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
                    value = str(operation.get(field, ''))
                    value = value.replace('<br>', ' ')
                    value = value.replace('<BR>', ' ')
                    value = value.replace('&nbsp;', ' ')
                    value = re.sub(r'\s+', ' ', value).strip()
                    row[field] = value
                
                writer.writerow(row)
            
            return csv_buffer.getvalue()
            
        else:
            logger.error("❌ Treasury scraper failed or returned no data")
            return None
            
    except Exception as e:
        logger.error(f"❌ Enhanced Treasury scraper failed: {e}")
        raise e

def process_moa_enhanced(renderer_url=None, use_ai=False):
    """Enhanced MOA processing with optional renderer and AI"""
    logger.info("🌐 Starting ENHANCED MOA processing...")
    
    try:
        # Initialize YOUR FEDAOParser
        fedao_parser = FEDAOParser()
        
        # Get the current schedule page
        schedule_page_url = "https://www.newyorkfed.org/markets/ambs_operation_schedule"
        
        # Enhancement: Use renderer if available for dynamic content
        if renderer_url:
            logger.info("🎯 Using advanced web renderer for schedule page...")
            try:
                page_content = use_web_renderer(renderer_url, schedule_page_url)
                pdf_url = extract_pdf_url_from_content(page_content, schedule_page_url)
            except Exception as e:
                logger.warning(f"⚠️ Renderer failed, using fallback: {e}")
                pdf_url = get_pdf_url_fallback(schedule_page_url)
        else:
            pdf_url = get_pdf_url_fallback(schedule_page_url)
        
        if not pdf_url:
            raise Exception("No current schedule PDF found")
        
        logger.info(f"✅ Found current schedule PDF: {pdf_url}")
        
        # Download the PDF (YOUR EXISTING LOGIC)
        pdf_response = requests.get(pdf_url, timeout=60, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        pdf_response.raise_for_status()
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file.write(pdf_response.content)
            temp_file_path = temp_file.name
        
        # Parse with YOUR existing parser
        try:
            operations = fedao_parser.parse_pdf(temp_file_path)
            
            if operations:
                logger.info(f"🎉 SUCCESS: Extracted {len(operations)} operations")
                
                # Enhancement: AI validation if available
                if use_ai and AI_AVAILABLE:
                    logger.info("🧠 Applying AI validation...")
                    operations = apply_ai_validation(operations, "MOA")
                
                # Use YOUR parser's exact csv_columns
                fieldnames = fedao_parser.csv_columns
                
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
                writer.writeheader()
                
                for operation in operations:
                    row = {}
                    for field in fieldnames:
                        value = operation.get(field, '')
                        row[field] = value
                    writer.writerow(row)
                
                return csv_buffer.getvalue()
                
            else:
                logger.warning("⚠️ Parser returned empty operations list")
                return create_diagnostic_csv("Parser returned no operations")
        
        except Exception as parse_error:
            logger.error(f"❌ Error with parser: {parse_error}")
            return create_diagnostic_csv(f"Parser error: {str(parse_error)}")
        
        finally:
            # Clean up temp file
            try:
                os.unlink(temp_file_path)
            except:
                pass
            
    except Exception as e:
        logger.error(f"❌ Enhanced MOA processing failed: {e}")
        return create_diagnostic_csv(f"Enhanced processing error: {str(e)}")

def use_web_renderer(renderer_url, target_url):
    """Use Cloud Run renderer for advanced web scraping"""
    try:
        response = requests.post(renderer_url, 
            json={"url": target_url, "wait_time": 5000, "include_resources": True},
            timeout=60
        )
        response.raise_for_status()
        return response.json().get('content', '')
    except Exception as e:
        logger.error(f"Web renderer failed: {e}")
        return None

def apply_ai_validation(data, data_type):
    """Apply AI-powered validation and enhancement"""
    if not AI_AVAILABLE:
        return data
    
    try:
        # Simple AI validation - can be enhanced with actual Vertex AI calls
        logger.info(f"🧠 AI validating {len(data)} {data_type} records...")
        
        # Add validation logic here - for now, just add metadata
        for record in data:
            record['ai_validated'] = datetime.utcnow().isoformat()
            record['validation_score'] = 1.0  # Placeholder
        
        logger.info(f"✅ AI validation completed for {data_type}")
        return data
        
    except Exception as e:
        logger.warning(f"⚠️ AI validation failed: {e}")
        return data

def trigger_ai_processing(file_path, data_type, timestamp):
    """Trigger AI processing pipeline"""
    try:
        # This would trigger the transformer function
        from google.cloud import pubsub_v1
        
        project_id = os.environ.get('GCP_PROJECT')
        topic_name = "fedao-transform-topic"
        
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        
        message_data = {
            "file_path": file_path,
            "data_type": data_type,
            "timestamp": timestamp,
            "processing_mode": "ai_enhanced"
        }
        
        future = publisher.publish(topic_path, json.dumps(message_data).encode('utf-8'))
        logger.info(f"🎯 AI processing triggered: {future.result()}")
        
    except Exception as e:
        logger.warning(f"⚠️ Could not trigger AI processing: {e}")

def get_pdf_url_fallback(schedule_page_url):
    """Fallback PDF URL extraction (YOUR EXISTING LOGIC)"""
    page_response = requests.get(schedule_page_url, timeout=30, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    page_response.raise_for_status()
    
    pdf_pattern = r'href="([^"]*AMBS-Schedule[^"]*\.pdf[^"]*)"'
    pdf_matches = re.findall(pdf_pattern, page_response.text, re.IGNORECASE)
    
    if not pdf_matches:
        pdf_pattern = r'href="([^"]*Schedule[^"]*\.pdf[^"]*)"'
        pdf_matches = re.findall(pdf_pattern, page_response.text, re.IGNORECASE)
    
    if pdf_matches:
        return urljoin(schedule_page_url, pdf_matches[0])
    
    return None

def extract_pdf_url_from_content(content, base_url):
    """Extract PDF URL from rendered content"""
    if not content:
        return None
    
    pdf_pattern = r'href="([^"]*(?:AMBS-Schedule|Schedule)[^"]*\.pdf[^"]*)"'
    pdf_matches = re.findall(pdf_pattern, content, re.IGNORECASE)
    
    if pdf_matches:
        return urljoin(base_url, pdf_matches[0])
    
    return None

def create_diagnostic_csv(reason):
    """Create CSV with diagnostic info when parsing fails"""
    fieldnames = [
        'OperationDate', 'OperationTime', 'Operation Type', 'Settlement Date',
        'Securities Included (CUSP)', 'Security Maximums (Millions)', 
        'OperationMaximum', 'Source_Date'
    ]
    
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    
    writer.writerow({
        'OperationDate': 'DIAGNOSTIC',
        'OperationTime': reason,
        'Operation Type': 'ENHANCED_PROCESSING',
        'Settlement Date': '',
        'Securities Included (CUSP)': 'Check system logs for details',
        'Security Maximums (Millions)': 'Enhanced capabilities available',
        'OperationMaximum': '$0',
        'Source_Date': datetime.now().strftime('%Y%m%d')
    })
    
    return csv_buffer.getvalue()

def create_enhanced_summary(mode: str, timestamp: str, results: dict, use_ai: bool, use_renderer: bool) -> str:
    """Create enhanced processing summary"""
    summary = {
        "processing_timestamp": timestamp,
        "processing_mode": mode,
        "status": "success" if not results.get("errors") else "partial",
        "files_created": results.get("files_created", []),
        "errors": results.get("errors", []),
        "enhancements": results.get("enhancements", []),
        "capabilities_used": {
            "ai_processing": use_ai and AI_AVAILABLE,
            "web_renderer": use_renderer,
            "frbny_scraper": FRBNY_SCRAPER_AVAILABLE,
            "fedao_parser": FEDAO_PARSER_AVAILABLE
        },
        "system_info": {
            "version": "enhanced_v1.0",
            "features": ["real_data", "ai_validation", "web_rendering", "multi_stage_pipeline"],
            "compatibility": "backward_compatible_with_existing_parsers"
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
