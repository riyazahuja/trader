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
# Using separate terminals
python ingest.py &
python nlp_engine.py &
python selector.py &
python executor.py &
python monitor.py
```

Or with tmux:

```bash
# Create new tmux session
tmux new-session -d -s trader

# Create windows for each module
tmux new-window -t trader -n ingest 'python ingest.py'
tmux new-window -t trader -n nlp 'python nlp_engine.py'
tmux new-window -t trader -n selector 'python selector.py'
tmux new-window -t trader -n executor 'python executor.py'
tmux new-window -t trader -n monitor 'python monitor.py'

# Attach to session
tmux attach-session -t trader
```

Each service communicates through in-memory queues and uses Alpaca's paper trading environment.
