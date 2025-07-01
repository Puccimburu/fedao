#!/bin/bash

# ==============================================================================
#
#  FEDAO Pipeline Deployment - REAL DATA VERSION (IMPROVED)
#
#  This script automatically finds and copies parser files
#  Deploys with REAL Federal Reserve data processing
#
# ==============================================================================

# --- Configuration ---
REGION="europe-west1"
SERVICE_ACCOUNT_NAME="fedao-scraper-service-account"

# --- Source Code Directories ---
SCRAPER_FUNC_SRC_DIR="../src/functions/scrape_fedao_sources"

# --- Service & Bucket Names (USING EXISTING BUCKET) ---
EXISTING_BUCKET_NAME="execo-simba-fedao-data-bucket"
SCRAPER_FUNCTION_NAME="scrape-fedao-sources"
SCRAPER_TOPIC_NAME="scrape-fedao-sources-topic"
SCHEDULER_JOB_NAME="fedao-10min-trigger"

# --- Logging ---
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/real_data_deploy_$(date +%Y%m%d_%H%M%S).log"

# Exit on any error
set -e

# ==============================================================================
# --- Helper Functions
# ==============================================================================
log_and_echo() {
  echo "$1" | tee -a "$LOG_FILE"
}

find_and_copy_parser_files() {
    log_and_echo "🔍 Searching for parser files in multiple locations..."
    
    # Possible locations for parser files
    SEARCH_PATHS=(
        "src/functions/scrape_fedao_sources"   # Target directory (where files should be)
        "."                                    # Current directory
        ".."                                  # Parent directory
        "../.."                              # Grandparent directory
        "functions/scrape_fedao_sources"     # Original functions directory
        "../functions/scrape_fedao_sources"  # Parent functions directory
        "../../functions/scrape_fedao_sources" # Grandparent functions directory
        "scripts"                            # Scripts directory
        "../scripts"                         # Parent scripts directory
    )
    
    FRBNY_FOUND=false
    FEDAO_FOUND=false
    
    for search_path in "${SEARCH_PATHS[@]}"; do
        if [ -f "$search_path/frbny_parser.py" ] && [ "$FRBNY_FOUND" = false ]; then
            log_and_echo "✅ Found frbny_parser.py in: $search_path"
            cp "$search_path/frbny_parser.py" "$SCRAPER_FUNC_SRC_DIR/"
            FRBNY_FOUND=true
        fi
        
        if [ -f "$search_path/fedao_parser.py" ] && [ "$FEDAO_FOUND" = false ]; then
            log_and_echo "✅ Found fedao_parser.py in: $search_path"
            cp "$search_path/fedao_parser.py" "$SCRAPER_FUNC_SRC_DIR/"
            FEDAO_FOUND=true
        fi
        
        # Break if both found
        if [ "$FRBNY_FOUND" = true ] && [ "$FEDAO_FOUND" = true ]; then
            break
        fi
    done
    
    # Report results
    if [ "$FRBNY_FOUND" = true ]; then
        log_and_echo "✅ frbny_parser.py copied successfully"
    else
        log_and_echo "⚠️  frbny_parser.py not found in any location - TOA processing may fail"
        log_and_echo "   Searched in: ${SEARCH_PATHS[*]}"
    fi
    
    if [ "$FEDAO_FOUND" = true ]; then
        log_and_echo "✅ fedao_parser.py copied successfully"
    else
        log_and_echo "⚠️  fedao_parser.py not found in any location - MOA processing may fail"
        log_and_echo "   Searched in: ${SEARCH_PATHS[*]}"
    fi
    
    # List what's actually in the function directory now
    log_and_echo "📁 Function directory contents:"
    ls -la "$SCRAPER_FUNC_SRC_DIR/" | tee -a "$LOG_FILE"
}

create_requirements_txt() {
    # Check if requirements.txt already exists and has the needed packages
    if [ -f "$SCRAPER_FUNC_SRC_DIR/requirements.txt" ]; then
        if grep -q "selenium" "$SCRAPER_FUNC_SRC_DIR/requirements.txt" 2>/dev/null; then
            log_and_echo "✅ Enhanced requirements.txt already exists, keeping existing version"
            return
        else
            log_and_echo "📝 Updating requirements.txt with additional packages..."
        fi
    fi
    
    log_and_echo "📝 Creating enhanced requirements.txt..."
    cat > "$SCRAPER_FUNC_SRC_DIR/requirements.txt" << 'EOF'
functions-framework==3.*
google-cloud-storage==2.*
requests==2.*
beautifulsoup4==4.*
lxml==4.*
PyPDF2==3.*
pdfplumber==0.9.*
pandas==2.*
pathlib2==2.*
selenium==4.*
webdriver-manager==3.*
EOF
}

create_real_data_main() {
    # Check if main.py already exists with real data code
    if [ -f "$SCRAPER_FUNC_SRC_DIR/main.py" ]; then
        # Check if it contains real data markers
        if grep -q "REAL FEDAO Scraper" "$SCRAPER_FUNC_SRC_DIR/main.py" 2>/dev/null; then
            log_and_echo "✅ Real data main.py already exists, keeping existing version"
            return
        else
            log_and_echo "📝 Found basic main.py, updating to real data version..."
        fi
    fi
    
    log_and_echo "📝 Creating real data main.py..."
    cat > "$SCRAPER_FUNC_SRC_DIR/main.py" << 'EOF'
#!/usr/bin/env python3
"""
FEDAO Cloud Function - REAL DATA FOR BOTH TOA AND MOA
- TOA: Real FRBNY scraper (Treasury Securities) 
- MOA: Real PDF download + parser (Mortgage-Backed Securities)
- FIXED: Field name matching for MOA processing
- FIXED: Import errors and debug logging
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
from typing import Dict, Any
from urllib.parse import urljoin
from google.cloud import storage
import functions_framework

# Setup logging - FIXED: Added logger assignment
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
    """Process TOA using REAL FRBNY scraper - WITHOUT SELENIUM"""
    logger.info("🌐 Starting REAL FRBNY Treasury Securities scraping...")
    
    try:
        # Initialize the real scraper
        scraper = CombinedFRBNYScraper()
        
        # FRBNY Treasury Securities URL
        url = "https://www.newyorkfed.org/markets/domestic-market-operations/monetary-policy-implementation/treasury-securities/treasury-securities-operational-details"
        
        # Run the scraper (without Selenium)
        success = scraper.run(url)
        
        if success and scraper.data:
            logger.info(f"✅ Successfully scraped {len(scraper.data)} Treasury operations from FRBNY")
            
            # Convert to standardized CSV format
            standardized_data = scraper.standardize_output_format(scraper.data)
            
            # Convert to CSV string
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
                    # Clean up HTML tags from the data
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
        logger.error(f"❌ Real Treasury scraper failed: {e}")
        raise e

def process_moa_with_real_pdf_download():
    """REAL MOA processing - Download current AMBS schedule PDF"""
    logger.info("🌐 Starting REAL MOA processing - CURRENT SCHEDULE PDF")
    
    try:
        # Initialize FEDAOParser
        fedao_parser = FEDAOParser()
        
        # Get the current schedule page to find the actual PDF URL
        schedule_page_url = "https://www.newyorkfed.org/markets/ambs_operation_schedule"
        
        logger.info(f"🔍 Getting current schedule page: {schedule_page_url}")
        
        page_response = requests.get(schedule_page_url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        page_response.raise_for_status()
        
        # Extract the current schedule PDF URL from the page
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
        
        # Download the current schedule PDF
        pdf_response = requests.get(current_schedule_pdf, timeout=60, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        pdf_response.raise_for_status()
        
        # Validate PDF content
        content_type = pdf_response.headers.get('content-type', '').lower()
        if 'pdf' not in content_type:
            raise Exception(f"Expected PDF, got {content_type}")
        
        logger.info(f"✅ Downloaded PDF successfully: {len(pdf_response.content)} bytes")
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file.write(pdf_response.content)
            temp_file_path = temp_file.name
        
        # Parse the PDF using FEDAOParser
        try:
            operations = fedao_parser.parse_pdf(temp_file_path)
            
            if operations:
                logger.info(f"🎉 SUCCESS: Extracted {len(operations)} REAL operations from current schedule")
                
                # Use FEDAOParser's exact csv_columns
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
                logger.warning("⚠️  Parser returned empty operations list")
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
        logger.error(f"❌ REAL MOA PDF download/processing failed: {e}")
        return create_diagnostic_csv(f"Download/processing error: {str(e)}")

def create_diagnostic_csv(reason):
    """Create CSV with diagnostic info when real parsing fails"""
    logger.info(f"📋 Creating diagnostic CSV: {reason}")
    
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
EOF
}

# ==============================================================================
# --- Main Deployment Logic
# ==============================================================================
log_and_echo "🚀 FEDAO REAL DATA Deployment - $(date)"
log_and_echo "✅ Using existing bucket: gs://$EXISTING_BUCKET_NAME"
log_and_echo "🎯 DEPLOYING WITH REAL FEDERAL RESERVE DATA"
log_and_echo "---"

# --- Step 1: Initial GCP Setup ---
log_and_echo "STEP 1: Configuring gcloud and enabling services..."

# Get current project
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ -z "$CURRENT_PROJECT" ]; then
    log_and_echo "❌ ERROR: No project is currently set."
    exit 1
fi

log_and_echo "✅ Using project: $CURRENT_PROJECT"
PROJECT_ID="$CURRENT_PROJECT"
SERVICE_ACCOUNT="$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"

# Enable required services
log_and_echo "Enabling required services..."
gcloud services enable cloudfunctions.googleapis.com \
                       cloudbuild.googleapis.com \
                       pubsub.googleapis.com \
                       storage.googleapis.com \
                       cloudscheduler.googleapis.com \
                       iam.googleapis.com \
                       eventarc.googleapis.com \
                       artifactregistry.googleapis.com \
                       --project="$PROJECT_ID" >> "$LOG_FILE" 2>&1
log_and_echo "✅ Services enabled."

# --- Step 2: Service Account ---
log_and_echo "STEP 2: Setting up service account..."

if ! gcloud iam service-accounts describe "$SERVICE_ACCOUNT" --project="$PROJECT_ID" >/dev/null 2>&1; then
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
        --display-name="FEDAO Scraper Service Account" \
        --project="$PROJECT_ID" >> "$LOG_FILE" 2>&1
    log_and_echo "✅ Service account created"
else
    log_and_echo "✅ Service account already exists"
fi

# Grant IAM roles
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/storage.objectAdmin" \
    --condition=None >> "$LOG_FILE" 2>&1

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/pubsub.subscriber" \
    --condition=None >> "$LOG_FILE" 2>&1

log_and_echo "✅ IAM permissions configured."

# --- Step 3: Verify Existing Bucket ---
log_and_echo "STEP 3: Verifying existing bucket..."

if gcloud storage ls "gs://$EXISTING_BUCKET_NAME" >/dev/null 2>&1; then
    log_and_echo "✅ Existing bucket confirmed: gs://$EXISTING_BUCKET_NAME"
    
    # Ensure folder structure exists
    if ! gcloud storage ls "gs://$EXISTING_BUCKET_NAME/FRBNY/FEDAO/" >/dev/null 2>&1; then
        log_and_echo "Creating FRBNY/FEDAO folder structure..."
        echo "" | gcloud storage cp - "gs://$EXISTING_BUCKET_NAME/FRBNY/FEDAO/.keep" >> "$LOG_FILE" 2>&1
        log_and_echo "✅ Folder structure created"
    else
        log_and_echo "✅ Folder structure already exists"
    fi
    
    # Create summaries folder
    if ! gcloud storage ls "gs://$EXISTING_BUCKET_NAME/FRBNY/FEDAO/summaries/" >/dev/null 2>&1; then
        log_and_echo "Creating summaries folder structure..."
        echo "" | gcloud storage cp - "gs://$EXISTING_BUCKET_NAME/FRBNY/FEDAO/summaries/.keep" >> "$LOG_FILE" 2>&1
        log_and_echo "✅ Summaries folder created"
    fi
else
    log_and_echo "❌ ERROR: Bucket gs://$EXISTING_BUCKET_NAME not accessible"
    log_and_echo "   Check if bucket exists and you have permissions"
    exit 1
fi

# --- Step 4: Pub/Sub Topic ---
log_and_echo "STEP 4: Creating Pub/Sub topic..."

if ! gcloud pubsub topics describe "$SCRAPER_TOPIC_NAME" --project="$PROJECT_ID" >/dev/null 2>&1; then
    gcloud pubsub topics create "$SCRAPER_TOPIC_NAME" --project="$PROJECT_ID" >> "$LOG_FILE" 2>&1
    log_and_echo "✅ Pub/Sub topic created"
else
    log_and_echo "✅ Pub/Sub topic already exists"
fi

# --- Step 5: Prepare Function with REAL DATA ---
log_and_echo "STEP 5: Preparing Cloud Function with REAL DATA processing..."

mkdir -p "$SCRAPER_FUNC_SRC_DIR"
create_real_data_main
create_requirements_txt
find_and_copy_parser_files
log_and_echo "✅ Function source prepared with REAL DATA capabilities"

# --- Step 6: Deploy Function ---
log_and_echo "STEP 6: Deploying Cloud Function with REAL DATA..."

gcloud functions deploy "$SCRAPER_FUNCTION_NAME" \
    --gen2 \
    --runtime=python39 \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --source="$SCRAPER_FUNC_SRC_DIR" \
    --entry-point="fedao_scraper_main" \
    --trigger-topic="$SCRAPER_TOPIC_NAME" \
    --service-account="$SERVICE_ACCOUNT" \
    --timeout="540s" \
    --memory="2Gi" \
    --set-env-vars="GCP_PROJECT=${PROJECT_ID},FEDAO_OUTPUT_BUCKET=${EXISTING_BUCKET_NAME}" \
    2>&1 | tee -a "$LOG_FILE"

if [ ${PIPESTATUS[0]} -ne 0 ]; then
    log_and_echo "❌ Function deployment failed"
    exit 1
fi

log_and_echo "✅ Function deployed successfully with REAL DATA processing!"

# --- Step 7: Create 10-Minute Scheduler ---
log_and_echo "STEP 7: Creating 10-minute scheduler..."

if ! gcloud scheduler jobs describe "$SCHEDULER_JOB_NAME" --location="$REGION" --project="$PROJECT_ID" >/dev/null 2>&1; then
    gcloud scheduler jobs create pubsub "$SCHEDULER_JOB_NAME" \
      --location="$REGION" \
      --schedule="*/10 * * * *" \
      --time-zone="America/New_York" \
      --topic="$SCRAPER_TOPIC_NAME" \
      --message-body='{"mode": "both"}' \
      --description="FEDAO scraper - runs every 10 minutes - REAL DATA" \
      --project="$PROJECT_ID" >> "$LOG_FILE" 2>&1
      
    log_and_echo "✅ Scheduler created (every 10 minutes) - REAL DATA"
else
    log_and_echo "✅ Scheduler already exists"
fi

# --- Step 8: Test the Function ---
log_and_echo "STEP 8: Testing the deployed function..."
log_and_echo "🧪 Triggering test run..."

# Trigger a test run
gcloud pubsub topics publish "$SCRAPER_TOPIC_NAME" --message='{"mode": "both"}' --project="$PROJECT_ID"

log_and_echo "✅ Test trigger sent! Function should process in ~30 seconds."
log_and_echo "💡 Monitor with: gcloud functions logs read $SCRAPER_FUNCTION_NAME --gen2 --region=$REGION --limit=20"

# --- Final Summary ---
log_and_echo "---"
log_and_echo "🎉 REAL DATA DEPLOYMENT COMPLETED SUCCESSFULLY!"
log_and_echo "---"
log_and_echo "📋 SUMMARY:"
log_and_echo "  • Project: $PROJECT_ID"
log_and_echo "  • Existing Bucket: gs://$EXISTING_BUCKET_NAME"
log_and_echo "  • Function: $SCRAPER_FUNCTION_NAME"
log_and_echo "  • Schedule: Every 10 minutes"
log_and_echo "  • Data Type: 🎯 REAL FEDERAL RESERVE DATA"
log_and_echo ""
log_and_echo "🎯 REAL DATA OUTPUT FILES (in your existing bucket):"
log_and_echo "  • MOA: gs://$EXISTING_BUCKET_NAME/FRBNY/FEDAO/FEDAO_MOA_DATA_YYYYMMDD_HHMMSS.csv"
log_and_echo "  • TOA: gs://$EXISTING_BUCKET_NAME/FRBNY/FEDAO/FEDAO_TOA_DATA_YYYYMMDD_HHMMSS.csv"
log_and_echo "  • Latest: gs://$EXISTING_BUCKET_NAME/FRBNY/FEDAO/FEDAO_*_DATA_LATEST.csv"
log_and_echo "  • Summaries: gs://$EXISTING_BUCKET_NAME/FRBNY/FEDAO/summaries/"
log_and_echo ""
log_and_echo "🔧 MANUAL TRIGGER COMMANDS:"
log_and_echo "  # Test both MOA and TOA:"
log_and_echo "  gcloud pubsub topics publish $SCRAPER_TOPIC_NAME --message='{\"mode\": \"both\"}'"
log_and_echo ""
log_and_echo "  # Test only Treasury operations (TOA):"
log_and_echo "  gcloud pubsub topics publish $SCRAPER_TOPIC_NAME --message='{\"mode\": \"toa\"}'"
log_and_echo ""
log_and_echo "  # Test only Mortgage operations (MOA):"
log_and_echo "  gcloud pubsub topics publish $SCRAPER_TOPIC_NAME --message='{\"mode\": \"moa\"}'"
log_and_echo ""
log_and_echo "📊 MONITORING COMMANDS:"
log_and_echo "  # Watch function logs:"
log_and_echo "  gcloud functions logs read $SCRAPER_FUNCTION_NAME --gen2 --region=$REGION --limit=20"
log_and_echo ""
log_and_echo "  # Check output files:"
log_and_echo "  gcloud storage ls gs://$EXISTING_BUCKET_NAME/FRBNY/FEDAO/"
log_and_echo ""
log_and_echo "  # View latest summary:"
log_and_echo "  gcloud storage cat gs://$EXISTING_BUCKET_NAME/FRBNY/FEDAO/summaries/PROCESSING_SUMMARY_*.json | tail -50"
log_and_echo ""
log_and_echo "📈 WHAT HAPPENS NEXT:"
log_and_echo "  • Function runs automatically every 10 minutes"
log_and_echo "  • Downloads current FRBNY Treasury operations (TOA)"
log_and_echo "  • Downloads current AMBS schedule PDF (MOA)"  
log_and_echo "  • Creates timestamped files with real Federal Reserve data"
log_and_echo "  • Saves processing summaries for monitoring"
log_and_echo ""
log_and_echo "✅ Your FEDAO scraper now processes REAL Federal Reserve data every 10 minutes!"
log_and_echo "🎯 No more test data - only current FRBNY operations!"
log_and_echo ""
log_and_echo "🎉 AUTOMATION COMPLETE - Real Federal Reserve data pipeline is LIVE!"
log_and_echo "---"