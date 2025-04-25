from flask import Flask, request, jsonify
import json
import os
import glob

app = Flask(__name__)

# Load processed data from JSON files
def load_data():
    data = []
    json_files = glob.glob("processed_data/part-*.json")
    
    for file_path in json_files:
        with open(file_path, 'r') as file:
            for line in file:
                try:
                    record = json.loads(line)
                    data.append(record)
                except json.JSONDecodeError:
                    continue
    
    return data

# Global data variable to avoid reloading for every request
processed_data = None

@app.before_first_request
def initialize():
    global processed_data
    processed_data = load_data()

@app.route('/results', methods=['POST'])
def get_results():
    global processed_data
    if processed_data is None:
        processed_data = load_data()
    
    request_data = request.get_json()
    search_term = request_data.get('term')
    
    if not search_term:
        return jsonify({"error": "Search term is required"}), 400
    
    # Filter records by search term, stripping quotes from stored terms
    results = {}
    term_records = [r for r in processed_data if r['term'][1:-1] == search_term]
    
    for record in term_records:
        url = record['url']
        clicks = record['clicks']
        results[url] = clicks
    
    # Sort by click count (descending) and then by domain priority
    def sort_key(item):
        url, clicks = item
        domain_priority = 0
        if url.endswith('.org'):
            domain_priority = 3
        elif url.endswith('.edu'):
            domain_priority = 2
        elif url.endswith('.com'):
            domain_priority = 1
        return (-clicks, -domain_priority)
    
    sorted_results = dict(sorted(results.items(), key=sort_key))
    
    return jsonify({"results": sorted_results})

@app.route('/trends', methods=['POST'])
def get_trends():
    global processed_data
    if processed_data is None:
        processed_data = load_data()
    
    request_data = request.get_json()
    search_term = request_data.get('term')
    
    if not search_term:
        return jsonify({"error": "Search term is required"}), 400
    
    # Calculate total clicks for the search term, stripping quotes from stored terms
    total_clicks = sum(r['clicks'] for r in processed_data if r['term'][1:-1] == search_term)
    
    return jsonify({"clicks": total_clicks})

@app.route('/popularity', methods=['POST'])
def get_popularity():
    global processed_data
    if processed_data is None:
        processed_data = load_data()
    
    request_data = request.get_json()
    url = request_data.get('url')
    
    if not url:
        return jsonify({"error": "URL is required"}), 400
    
    # Calculate total clicks for the URL
    total_clicks = sum(r['clicks'] for r in processed_data if r['url'] == url)
    
    return jsonify({"clicks": total_clicks})

@app.route('/getBestTerms', methods=['POST'])
def get_best_terms():
    global processed_data
    if processed_data is None:
        processed_data = load_data()
    
    request_data = request.get_json()
    website = request_data.get('website')
    
    if not website:
        return jsonify({"error": "Website URL is required"}), 400
    
    # Calculate total clicks for the website
    website_records = [r for r in processed_data if r['url'] == website]
    total_website_clicks = sum(r['clicks'] for r in website_records)
    
    if total_website_clicks == 0:
        return jsonify({"best_terms": []})
    
    # Find terms where website received more than 5% of total clicks
    best_terms = []
    url_term_clicks = {}
    
    # Group clicks by term for this URL
    for record in website_records:
        # Strip quotes from the term
        term = record['term'][1:-1]
        clicks = record['clicks']
        if term in url_term_clicks:
            url_term_clicks[term] += clicks
        else:
            url_term_clicks[term] = clicks
    
    # Check which terms meet the 5% threshold
    for term, clicks in url_term_clicks.items():
        if clicks / total_website_clicks > 0.05:
            best_terms.append(term)
    
    return jsonify({"best_terms": best_terms})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3000, debug=True) 