#!/bin/bash
# Setup GitHub CI/CD for Data Agent
# This script configures GitHub secrets for automated deployment

set -e

echo "🔧 Data Agent CI/CD Setup"
echo "=========================="
echo ""

# Check if gh CLI is installed
if ! command -v gh &> /dev/null; then
    echo "❌ GitHub CLI (gh) is not installed"
    echo "Install it with: sudo apt install gh"
    exit 1
fi

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "❌ .env file not found"
    exit 1
fi

REPO="${GITHUB_PIPELINES_OWNER:-sam2881}/${GITHUB_PIPELINES_REPO:-enterprise-data-pipelines}"

echo "Repository: $REPO"
echo ""

# Check authentication
echo "Checking GitHub authentication..."
if ! gh auth status &> /dev/null; then
    echo "❌ Not authenticated with GitHub"
    echo "Run: gh auth login"
    exit 1
fi

echo "✅ Authenticated"
echo ""

# Set GitHub secrets
echo "Setting GitHub secrets..."

# GCP Project ID
if [ -n "$GCP_PROJECT_ID" ]; then
    echo "Setting GCP_PROJECT_ID..."
    echo "$GCP_PROJECT_ID" | gh secret set GCP_PROJECT_ID --repo="$REPO"
    echo "✅ GCP_PROJECT_ID set"
fi

# GCP Service Account Key
if [ -f "/tmp/gcp-sa-key.json" ]; then
    echo "Setting GCP_SA_KEY from /tmp/gcp-sa-key.json..."
    gh secret set GCP_SA_KEY --repo="$REPO" < /tmp/gcp-sa-key.json
    echo "✅ GCP_SA_KEY set"
else
    echo "⚠️  /tmp/gcp-sa-key.json not found - you'll need to set GCP_SA_KEY manually"
fi

# GCS Bucket names
echo "Setting GCS bucket names..."
echo "agent-ai-test-461120-airflow-dags" | gh secret set GCS_DAG_BUCKET --repo="$REPO"
echo "agent-ai-test-461120-temp" | gh secret set GCS_SPARK_BUCKET --repo="$REPO"
echo "✅ GCS bucket names set"

echo ""
echo "🎉 GitHub CI/CD setup complete!"
echo ""
echo "Next steps:"
echo "1. The workflow file is at: .github/workflows/data-agent-cicd.yml"
echo "2. It will be deployed on the next pipeline generation"
echo "3. After that, all PRs from data-agent/* will:"
echo "   ✅ Auto-validate DAG imports"
echo "   ✅ Auto-merge if tests pass"
echo "   ✅ Auto-deploy to GCS"
echo "   ✅ Auto-rollback on failure"
echo ""
echo "Test it by filling the UI form and generating a pipeline!"
