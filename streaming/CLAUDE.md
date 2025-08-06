# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

This project uses `uv` as the package manager. Common commands:

- **Install dependencies**: `uv sync`
- **Run the streaming service**: `uv run python main.py`
- **Run once (exit after processing current file)**: `uv run python main.py --once`
- **Run tests**: `pytest` (when tests are added)

## High-Level Architecture

This is a **Hyperliquid node log streaming system (v2.5)** designed for ultra-fast, real-time ingestion of trading data from Hyperliquid node logs into Redis streams.

### Core Components:

1. **Main Entry Point** (`main.py`):
   - **Time-based file discovery**: Calculates expected log file paths from UTC time
   - **Deterministic rotation**: Handles hourly log rotation with predictable path calculation
   - **Real-time streaming**: Tails log files and streams new lines to Redis
   - **Production-ready**: Zero data loss, bounded memory, robust error handling

2. **Data Flow**:
   - Hyperliquid node writes logs to hourly rotated files (format: `YYYYMMDD/HH`)
   - Streaming service calculates expected log file based on current UTC hour
   - During hour transitions (first 5 minutes), gracefully handles file switching
   - Each log line is pushed to Redis streams with batched pipeline operations
   - Consumers can process data from Redis in real-time

### Key Design Decisions (v2.5 Features):

- **Time-based discovery**: Replaces filesystem scanning with UTC-based path calculation
- **Deterministic rotation**: Predictable file switching based on time, not directory scanning
- **Redis reliability**: Proper retry limits, fresh pipelines for each batch, no data loss
- **Resource management**: Safe file descriptor handling with proper cleanup
- **Memory protection**: Bounded backlog queue (MAX_BACKLOG_SIZE=10000)
- **Race condition safety**: Final read after successor detection during rotation
- **Large-line safety**: 8 MiB limit with whole-record emission
- **Production stability**: Comprehensive error handling and logging

## Environment Variables

### Core Configuration
- `LOG_PATH`: Log directory path (default: `/home/hluser/hl/data/node_fills/hourly`)
- `REDIS_HOST`: Redis server host (default: `localhost`)
- `REDIS_PORT`: Redis server port (default: `6379`)
- `REDIS_PASSWORD`: Redis authentication password
- `REDIS_PASSWORD_FILE`: File containing Redis password
- `REDIS_STREAM_PREFIX`: Stream name prefix (default: `node_fills`)

### Performance Tuning
- `BUFFER_SIZE`: File read buffer size (default: `131072`)
- `PIPELINE_SIZE`: Redis pipeline batch size (default: `20`)
- `STREAM_MAXLEN`: Max stream length, 0=unlimited (default: `0`)
- `RECONNECT_DELAY_MS`: Redis reconnect delay (default: `500`)
- `MAX_LINE_SIZE`: Maximum line size in bytes (default: `8388608` = 8 MiB)
- `MAX_BACKLOG_SIZE`: Memory protection limit (default: `10000`)
- `LOG_LEVEL`: Logging verbosity (default: `INFO`)

## Docker Deployment

The service includes a Dockerfile for containerized deployment, designed to run alongside a Hyperliquid node with shared volume access to log directories. The container includes health checks and runs as non-root user `hluser`.