# Distributed Task Scheduler

A Node.js distributed task scheduler with worker polling and retry logic.

## Features

- Scheduler API with task queue
- Worker polling endpoint for task consumption
- Retry and requeue handling
- Health and dashboard monitoring
- In-memory fallback queue when Redis is unavailable
- Aesthetic web dashboard UI

## Files

- `app.js` - main scheduler API + cron task generation
- `worker.js` - worker process polling and processing tasks
- `index.html` - frontend dashboard UI
- `styles.css` - dashboard styles
- `package.json` - dependencies and npm scripts
- `Dockerfile`, `docker-compose.yml` - optional containerized runtime

## Quick Start

1. Install dependencies
```bash
npm install
```
2. Start scheduler
```bash
node app.js
```
3. Start worker in a new terminal
```bash
node worker.js
```
4. Open dashboard
- http://localhost:3000
5. Generate tasks quickly (for testing)
- http://localhost:3000/task/generate?count=8

## Endpoints

- `GET /health`
- `GET /dashboard?endpoint=stats`
- `GET /dashboard?endpoint=workers`
- `GET /worker/poll?workerId=...`
- `POST /worker/result`
- `POST /task/requeue`
- `POST /task/generate?count=N`

## Run with Docker (if Docker installed)

```bash
docker compose up --build
```
