# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

This project uses `uv` as the package manager. Common commands:

- **Install dependencies**: `uv sync`
- **Run the streaming service**: `uv run python main.py`
- **Run tests**: `pytest` (when tests are added)

## High-Level Architecture

This is a **Hyperliquid node log streaming system** designed for ultra-fast, real-time ingestion of trading data from Hyperliquid node logs into Redis streams.

### Core Components:

1. **Main Entry Point** (`main.py`):
   - Monitors Hyperliquid node log directories (e.g., `/home/hluser/hl/data/node_fills/hourly`)
   - Uses inotify for efficient file change detection
   - Implements automatic hourly log rotation detection
   - Tails log files in real-time and pushes new lines to Redis streams

2. **Data Flow**:
   - Hyperliquid node writes logs to hourly rotated files (format: `YYYYMMDD/HH`)
   - Streaming service tails the latest log file
   - Each log line is pushed to Redis streams (e.g., `node_fills:ALL`)
   - Consumers can process data from Redis in real-time

### Key Design Decisions:

- **Tail-mode implementation**: Focuses on streaming new data only, not historical processing
- **Hourly rotation handling**: Automatically detects and switches to new log files
- **Redis streams**: Chosen for reliability, persistence, and consumer group support
- **Minimal dependencies**: Uses only essential libraries for production stability (redis, inotify)

## Environment Variables

- `LOG_PATH`: Override the log directory path (default: `/home/hluser/hl/data/node_fills/hourly`)
- `REDIS_HOST`: Redis server host (default: `localhost`)
- `REDIS_PORT`: Redis server port (default: `6379`)

## Docker Deployment

The service includes a Dockerfile for containerized deployment, designed to run alongside a Hyperliquid node with shared volume access to log directories.