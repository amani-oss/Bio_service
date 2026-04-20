# 🌿 BioField — Biodiversity Observation Dashboard

A modern Flask web dashboard for the field observation pipeline.
Wraps `extract.py` and `map.py` into a unified UI with live log streaming.

---

## 📁 Project Structure

```
bio_dashboard/          ← this folder (the Flask app)
│
├── app.py              ← Flask web server
├── extract.py          ← AI extraction pipeline  (copy here)
├── map.py              ← Folium map generator    (copy here)
├── requirements.txt
├── .env                ← your API key (you create this)
│
├── templates/          ← Jinja2 HTML pages
│   ├── base.html
│   ├── index.html
│   ├── observations.html
│   ├── map_view.html
│   └── pipeline.html
│
├── images_insects/     ← your insect photos  (you create these)
├── images_flora/       ← your flora photos
└── images_fungus/      ← your fungus photos
```

---

## 🚀 Step-by-Step: Run on Your Local PC

### Step 1 — Copy the original scripts

Copy `extract.py` and `map.py` into the `bio_dashboard/` folder
(alongside `app.py`). They must all be in the same directory.

### Step 2 — Create your image folders

```bash
mkdir images_insects images_flora images_fungus
```

Copy your field photos into the appropriate folders.
Supported formats: `.jpg`, `.jpeg`, `.png`

### Step 3 — Set up your API key

Create a `.env` file in the `bio_dashboard/` folder:

```
OPENROUTER_API_KEY=sk-or-xxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Get a free key at https://openrouter.ai

> Note: The map generator does NOT need an API key.
> Only `extract.py` calls the OpenRouter AI.

### Step 4 — Create a Python virtual environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python3 -m venv venv
source venv/bin/activate
```

### Step 5 — Install dependencies

```bash
pip install -r requirements.txt
```

### Step 6 — Run the Flask app

```bash
python app.py
```

You should see:
```
 * Running on http://127.0.0.1:5050
 * Debug mode: on
```

### Step 7 — Open the dashboard

Open your browser at: **http://localhost:5050**

---

## 🧭 Using the Dashboard

| Page | URL | What it does |
|---|---|---|
| Dashboard | `/` | Overview stats + recent observations |
| Observations | `/observations` | Full data table with search + filter |
| Field Map | `/map` | Embedded interactive Folium map |
| Pipeline | `/pipeline` | Run extract.py / map.py with live logs |

### Typical workflow:

1. Go to **Pipeline** → click **Run Extraction**
   - Logs stream in real-time in the terminal panel
   - Each image is EXIF-extracted + AI-identified
   - Results saved to `insecta_metadata.csv`, `flora_metadata.csv`, `fungus_metadata.csv`

2. Once extraction is done → click **Generate Map**
   - Creates `bio_observations_map.html`

3. Go to **Field Map** to view the interactive observation map

4. Go to **Observations** to browse + search all records

---

## ⚙️ Configuration

To change the port, edit the last line of `app.py`:
```python
app.run(debug=True, port=5050, threaded=True)
```

To change the AI model or rate limits, edit the constants in `extract.py`:
```python
MODEL = "google/gemma-3-4b-it:free"
BASE_REQUEST_DELAY = 30  # seconds between requests
```

---

## 🐛 Troubleshooting

**Port already in use:**
```bash
# Find and kill the process
lsof -i :5050        # macOS/Linux
netstat -ano | findstr :5050  # Windows
```

**`ModuleNotFoundError`:**
Make sure your virtual environment is activated and you ran `pip install -r requirements.txt`.

**AI identification fails:**
Check that `OPENROUTER_API_KEY` is set correctly in `.env`.
The pipeline uses a circuit breaker: after 3 consecutive failures it stops and saves progress.

**Map not loading in browser:**
The Folium HTML embeds images as base64 — it's fully self-contained.
If the iframe is blank, try opening `bio_observations_map.html` directly in your browser.
