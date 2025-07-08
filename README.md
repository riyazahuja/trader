# Trader Bot Prototype

This repository contains a minimal implementation of an automated trader prototype using free APIs. The design follows the specification from the project description.

## Requirements

- Python 3.9+
- API keys configured via environment variables:
  - `GEMINI_API_KEY`
  - `APCA_API_KEY_ID`
  - `APCA_API_SECRET_KEY`
  - `NEWSAPI_KEY`
  - `FINNHUB_KEY`

## Installation

Create a virtual environment and install dependencies (the bot relies on the
`alpaca-py` client library):

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running

Start each module in its own terminal or use a process manager such as `tmux`:

```bash
python ingest.py &
python nlp_engine.py &
python selector.py &
python executor.py &
python monitor.py
```

Each service communicates through in-memory queues and uses Alpaca's paper trading environment.
