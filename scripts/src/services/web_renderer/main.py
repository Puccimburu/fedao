from flask import Flask, request, jsonify
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import os

app = Flask(__name__)

def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    return webdriver.Chrome(options=chrome_options)

@app.route('/render', methods=['POST'])
def render_page():
    try:
        data = request.get_json()
        url = data.get('url')
        wait_time = data.get('wait_time', 3000)
        
        if not url:
            return jsonify({"error": "URL is required"}), 400
        
        driver = create_driver()
        try:
            driver.get(url)
            time.sleep(wait_time / 1000)  # Convert to seconds
            
            content = driver.page_source
            return jsonify({
                "status": "success",
                "content": content,
                "url": url
            })
        finally:
            driver.quit()
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
