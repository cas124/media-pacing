#!/bin/bash

# Configuration
PROJECT_ID="we-are-hipaa-smart"
REGION="us-central1"
JOB_NAME="learndash-daily-sync"
SECRET_NAME="wp-app-password"

echo "Deploying $JOB_NAME to Cloud Run Jobs..."

# 1. Deploy the Job
# --source .  : Uses the current folder
# --set-secrets : Maps the Google Secret to the env var 'WP_PASSWORD'
gcloud run jobs deploy $JOB_NAME \
  --source . \
  --region $REGION \
  --project $PROJECT_ID \
  --set-secrets="WP_PASSWORD=$SECRET_NAME:latest" \
  --max-retries 0 \
  --task-timeout 5m

echo "Deployment complete!"