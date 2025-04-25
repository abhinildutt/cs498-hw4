# PySpark Search Log Processing Assignment

This project processes search log data using PySpark and serves the processed data through a Flask API.

## Setup and Running

### Prerequisites
- PySpark installed on your VM
- Python 3.6+
- Flask

### Step 1: Process the Data

1. Upload the `searchLog.csv` file to your VM in the same directory as `preprocess.py`
2. Run the preprocessing script:
```
spark-submit preprocess.py
```
3. This will create a `processed_data` folder with the processed JSON files
4. Take a screenshot of the processed_data folder showing the _SUCCESS file and one of the part files with JSON contents for your README

### Step 2: Start the Flask API

1. Install dependencies:
```
pip install -r requirements.txt
```
2. Run the Flask application:
```
python app.py
```
3. The API will be available at http://YOUR-VM-IP:5000/

## API Endpoints

### 1. /results
Returns URLs and their click counts for a given search term.

**Request:**
```json
{
  "term": "your-search-term"
}
```

**Response:**
```json
{
  "results": {
    "url1.org": 150,
    "url2.edu": 120,
    "url3.com": 100
  }
}
```

### 2. /trends
Returns total clicks for a search term.

**Request:**
```json
{
  "term": "your-search-term"
}
```

**Response:**
```json
{
  "clicks": 370
}
```

### 3. /popularity
Returns total clicks for a URL.

**Request:**
```json
{
  "url": "example.com"
}
```

**Response:**
```json
{
  "clicks": 450
}
```

### 4. /getBestTerms
Returns search terms where the URL received more than 5% of its total clicks.

**Request:**
```json
{
  "website": "example.com"
}
```

**Response:**
```json
{
  "best_terms": ["term1", "term2"]
}
```

## Testing the Endpoints

You can test the endpoints using curl:

```bash
# Test /results endpoint
curl -X POST http://YOUR-VM-IP:5000/results -H "Content-Type: application/json" -d '{"term": "Portland"}'

# Test /trends endpoint
curl -X POST http://YOUR-VM-IP:5000/trends -H "Content-Type: application/json" -d '{"term": "Portland"}'

# Test /popularity endpoint
curl -X POST http://YOUR-VM-IP:5000/popularity -H "Content-Type: application/json" -d '{"url": "portlandonline.com"}'

# Test /getBestTerms endpoint
curl -X POST http://YOUR-VM-IP:5000/getBestTerms -H "Content-Type: application/json" -d '{"website": "portlandonline.com"}'
``` 