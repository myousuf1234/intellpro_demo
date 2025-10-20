# Document Ingestion Pipeline

A document management system that automatically ingests PDF files from Slack channels, extracts text and metadata, and stores the results in PostgreSQL for retrieval and analysis.

## Overview

This pipeline processes research reports and documents shared in Slack channels by:
1. Fetching messages and PDF attachments from Slack
2. Extracting text from PDFs using parallel processing
3. Extracting metadata (title, publication date) using OpenAI
4. Storing results in a PostgreSQL database

## Features

-  Slack API integration with pagination support
-  Parallel PDF processing using multiprocessing (bypasses Python's GIL)
-  OpenAI-powered metadata extraction
-  PostgreSQL storage with idempotent operations
-  Docker Compose for easy deployment
-  Comprehensive error handling and logging
-  Environment-based configuration (no hardcoded secrets)

## Prerequisites

- Docker and Docker Compose installed
- Slack Bot Token (xoxb-...)
- OpenAI API Key

## Slack App Setup

### 1. Create a Slack App

1. Go to https://api.slack.com/apps
2. Click "Create New App" → "From scratch"
3. Name your app (e.g., "Document Ingestor") and select your workspace

### 2. Configure Bot Token Scopes

Navigate to **OAuth & Permissions** and add these **Bot Token Scopes**:

- `channels:history` - Read messages from public channels
- `channels:read` - View basic channel information
- `files:read` - View files shared in channels

**Why these scopes?**
- `channels:history` - Required to read message history from the #research channel
- `channels:read` - Required to list channels and get channel IDs
- `files:read` - Required to download PDF file attachments

### 3. Install the App

1. In the **OAuth & Permissions** page, click "Install to Workspace"
2. Authorize the app
3. Copy the **Bot User OAuth Token** (starts with `xoxb-`)

### 4. Invite Bot to Channel

In Slack, go to your #research channel and type:
```
/invite @YourBotName
```

## Quick Start

### 1. Clone and Setup

```bash
# Clone the repository
git clone <repository-url>
cd intellpro_demo

# Copy environment template
cp .env.example .env
```

### 2. Configure Environment Variables

Edit `.env` with your actual credentials:

```bash
# Slack Configuration
SLACK_BOT_TOKEN=xoxb-your-actual-bot-token
SLACK_CHANNEL=research

# OpenAI Configuration
OPENAI_API_KEY=sk-proj-your-actual-openai-key

# PostgreSQL Configuration (default values work with Docker Compose)
POSTGRES_HOST=db
POSTGRES_PORT=5432
POSTGRES_DB=document_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Application Configuration
PDF_STORAGE_PATH=./extracted_pdfs
LOG_LEVEL=INFO
MAX_WORKERS=4
```

### 3. Run with Docker Compose

```bash
# Build and start services
docker-compose up --build

# Or run in detached mode
docker-compose up -d --build

# View logs
docker-compose logs -f app

# Stop services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

### 4. Running Multiple Times (Idempotency)

The pipeline is designed to be idempotent - you can run it multiple times safely:

```bash
# Run again to process any new messages
docker-compose up app
```

Existing documents (identified by Slack file_id) will be updated rather than duplicated.

## Architecture Decisions

### Database Schema Design

**Schema:**
```sql
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    file_id VARCHAR(255) UNIQUE NOT NULL,
    file_name VARCHAR(500) NOT NULL,
    title TEXT,
    publication_date DATE,
    extracted_text_path TEXT,
    text_length INTEGER,
    slack_url TEXT,
    message_ts VARCHAR(50),
    message_text TEXT,
    file_size INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Rationale:**
- `file_id` is the unique Slack file identifier, ensuring idempotency
- Separate storage of extracted text (filesystem) and metadata (database) for efficiency
- Timestamps enable audit trail and incremental processing
- Indexes on `file_id` and `processed_at` for fast lookups

### Multiprocessing Strategy

**Approach:** Process pool with configurable worker count

**Why this works:**
- PDF text extraction is CPU-bound (not I/O-bound)
- Python's GIL limits single-process performance for CPU tasks
- `multiprocessing.Pool` creates separate Python processes, each with its own GIL
- Each worker process extracts text from one PDF independently
- Worker count defaults to CPU cores (configurable via `MAX_WORKERS`)

**Implementation:**
```python
with Pool(processes=self.max_workers) as pool:
    processed_files = pool.map(process_single_pdf, files)
```

**Trade-offs:**
- Significant speedup for multiple PDFs
- Better resource utilization on multi-core systems
- Higher memory usage (one process per worker)
- Process creation overhead (minimal for PDF processing)

### Error Handling Approach

**Strategy:** Fail gracefully with comprehensive logging

- **API Rate Limits:** Exponential backoff retry for OpenAI API
- **Missing Data:** Continue processing with null values rather than failing
- **Network Errors:** Log and skip individual files, continue with batch
- **Database Errors:** Retry connections with timeout (30 attempts)

## Monitoring and Observability

### Logging

All components log to both console and `pipeline.log`:

```bash
# View logs in real-time
tail -f pipeline.log

# Or with Docker
docker-compose logs -f app
```

Log levels: ERROR, WARNING, INFO, DEBUG

## Future Improvements

### What would I do with more time?

1. **Incremental Processing**
   - Track last processed message timestamp
   - Only fetch new messages since last run
   - Implement checkpoint/resume functionality

2. **Async Processing with Job Queue**
   - Replace synchronous pipeline with async task queue (Celery + Redis)
   - Enable distributed processing across multiple workers
   - Better handling of long-running jobs

3. **API Endpoint**
   - FastAPI endpoint to query processed documents
   - Search by title, date range, content
   - Return document metadata and text

4. **Enhanced Error Recovery**
   - Retry failed documents in separate pass
   - Dead letter queue for persistent failures
   - Automated alerting for critical errors

5. **Testing**
   - Unit tests for each component
   - Integration tests with mock Slack/OpenAI APIs
   - Performance benchmarks

6. **Monitoring**
   - Prometheus metrics (documents/sec, error rates)
   - Grafana dashboards for visualization
   - Structured logging (JSON format) for better parsing

### Scaling to 10,000+ PDFs

**Architectural Changes:**

1. **Distributed Task Queue**
   - Use Celery with Redis/RabbitMQ for job distribution
   - Multiple worker nodes processing in parallel
   - Horizontal scaling based on queue depth

2. **Object Storage**
   - Move PDF storage from local filesystem to S3/MinIO
   - Reduces local storage requirements
   - Better durability and availability

3. **Batch Processing**
   - Process in configurable batch sizes (e.g., 100 at a time)
   - Reduces memory pressure
   - Better progress tracking

4. **Database Optimization**
   - Connection pooling (pgBouncer)
   - Batch inserts with COPY command
   - Read replicas for query workload

5. **Caching**
   - Cache Slack API responses (Redis)
   - Reduce redundant API calls
   - Faster reprocessing

6. **Resource Management**
   - Kubernetes for orchestration
   - Auto-scaling based on CPU/memory
   - Resource limits per worker

**Estimated Performance:**
- Current: ~10 PDFs/minute (single worker)
- With 10 workers: ~100 PDFs/minute
- With distributed queue: ~500+ PDFs/minute
- 10,000 PDFs: ~20-30 minutes with optimized setup

## Troubleshooting

### Common Issues

**"SLACK_BOT_TOKEN is required"**
- Ensure `.env` file exists and contains valid token
- Check token starts with `xoxb-`

**"Channel 'research' not found"**
- Verify channel name is correct (without #)
- Ensure bot is invited to the channel
- Try using channel ID directly

**"Database not ready"**
- Wait a few seconds for PostgreSQL to start
- Check `docker-compose logs db`

**"Rate limit hit"**
- OpenAI API rate limits exceeded
- Pipeline implements exponential backoff
- Consider upgrading OpenAI tier or reducing batch size

## Development

### Running Locally (without Docker)

```bash
# Install dependencies
pip install -r requirements.txt

# Set up PostgreSQL (or use Docker for just the DB)
docker run -d -p 5432:5432 \
  -e POSTGRES_DB=document_db \
  -e POSTGRES_PASSWORD=postgres \
  postgres:15-alpine

# Update .env with POSTGRES_HOST=localhost

# Run the pipeline
python main.py
```

