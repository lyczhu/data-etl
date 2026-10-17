Alertmanager Webhook Receiver

This small Flask app receives Alertmanager webhook POSTs and writes them to a log file.

Files:
- `monitoring/webhook/app.py` - Flask app
- `monitoring/webhook/Dockerfile` - Dockerfile
- `monitoring/webhook/requirements.txt` - Python deps

Log output location on host after `docker-compose up`:
- `monitoring/alert-logs/alerts.log`

To build and run just the webhook:

```bash
docker-compose up -d alert-webhook
```

To test sending a sample payload:

```bash
curl -X POST -H "Content-Type: application/json" -d '{"alerts":[{"status":"firing","labels":{"alertname":"TestAlert"}}]}' http://localhost:5001/alert
```

Check the log file for the received payload.
