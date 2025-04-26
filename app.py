from flask import Flask, request, jsonify, Response
import json
import os
import glob
from collections import OrderedDict

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
        
        # Ensure clicks is an integer and handle whitespace if it's a string
        clicks = record['clicks']
        if isinstance(clicks, str):
            clicks = int(clicks.strip())
        else:
            clicks = int(clicks)
            
        results[url] = clicks

    # First, sort by clicks in descending order
    results_sorted = sorted(results.items(), key=lambda x: x[1], reverse=True)
    
    # Print the sorted results
    print("Sorted results:", results_sorted)
    
    # Use OrderedDict to maintain insertion order
    sorted_results_dict = OrderedDict()
    for url, clicks in results_sorted:
        sorted_results_dict[url] = clicks
    
    print("Sorted results dict:", sorted_results_dict)
    
    # Create a direct JSON response to preserve order
    response_dict = {"results": dict(sorted_results_dict)}
    return Response(json.dumps(response_dict), mimetype='application/json')

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