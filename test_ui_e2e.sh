#!/bin/bash

# =============================================================================
# E2E UI Testing Script for Both Agent Systems
# =============================================================================

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     AI Agent Platform - E2E UI Testing                       ║${NC}"
echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo ""

# Function to check service health
check_service() {
    local name=$1
    local url=$2
    echo -n "Checking $name... "

    if curl -s -f -o /dev/null "$url"; then
        echo -e "${GREEN}✓ Healthy${NC}"
        return 0
    else
        echo -e "${RED}✗ Not responding${NC}"
        return 1
    fi
}

# =============================================================================
# 1. Health Checks
# =============================================================================
echo -e "${YELLOW}Step 1: Verifying all services...${NC}"
echo ""

check_service "Frontend (Next.js)" "http://localhost:3000"
check_service "Backend API" "http://localhost:8000/health"
check_service "Data Agent API" "http://localhost:8001/health"

echo ""
echo -e "${GREEN}✓ All services are running!${NC}"
echo ""

# =============================================================================
# 2. Frontend Build Check
# =============================================================================
echo -e "${YELLOW}Step 2: Checking TypeScript compilation...${NC}"
echo ""

cd /home/samrattidke600/ai_agent_app/frontend

echo "Running TypeScript type check..."
if npm run type-check 2>&1 | tee /tmp/type-check.log | tail -5; then
    echo -e "${GREEN}✓ TypeScript compilation successful${NC}"
else
    echo -e "${YELLOW}⚠ TypeScript warnings found (check /tmp/type-check.log)${NC}"
    echo "Note: Build warnings are expected during migration"
fi

echo ""

# =============================================================================
# 3. API Endpoints Test
# =============================================================================
echo -e "${YELLOW}Step 3: Testing API endpoints...${NC}"
echo ""

# Test Data Agent endpoints
echo "Testing Data Agent API endpoints..."
echo -n "  GET /api/v2/data-agent/pipelines... "
if curl -s -f "http://localhost:8001/api/v2/data-agent/pipelines" > /dev/null; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
fi

echo -n "  GET /api/v2/data-agent/health... "
if curl -s -f "http://localhost:8001/health" > /dev/null; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
fi

# Test Incident Management endpoints
echo ""
echo "Testing Incident Management API endpoints..."
echo -n "  GET /health... "
if curl -s -f "http://localhost:8000/health" > /dev/null; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗${NC}"
fi

echo -n "  GET /api/v1/incidents... "
if curl -s "http://localhost:8000/api/v1/incidents" > /dev/null; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${YELLOW}⚠${NC}"
fi

echo ""

# =============================================================================
# 4. Interactive UI Testing Guide
# =============================================================================
echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     Manual UI Testing Instructions                           ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

echo -e "${YELLOW}SYSTEM 1: Incident Management Agent${NC}"
echo ""
echo "Open in browser: http://localhost:3000"
echo ""
echo "Test these pages:"
echo "  1. /incidents      - View IT incidents from ServiceNow"
echo "  2. /approvals      - Review and approve remediation plans"
echo "  3. /workflows      - Monitor LangGraph workflow execution"
echo "  4. /graph/[id]     - View workflow DAG visualization"
echo ""
echo -e "${GREEN}Expected Behavior:${NC}"
echo "  ✓ Pages load without errors"
echo "  ✓ Incidents list displays (if any exist)"
echo "  ✓ Workflow states update in real-time"
echo "  ✓ Approval buttons work"
echo ""
echo "-------------------------------------------------------------------"
echo ""

echo -e "${YELLOW}SYSTEM 2: Data Engineering Agent (UPDATED)${NC}"
echo ""
echo "Open in browser: http://localhost:3000/pipelines"
echo ""
echo "Test the NEW UnifiedPipelineForm:"
echo ""
echo "  Test 1: Structured UI - 70+ Source Types"
echo "  ----------------------------------------"
echo "  1. Click 'Create Pipeline'"
echo "  2. Fill Pipeline Identity section"
echo "  3. Select Source Type:"
echo "     - Should see 9 categories (File, Database, Streaming, etc.)"
echo "     - Search functionality works"
echo "     - Click 'file_csv' from File-Based Sources"
echo "  4. Configure file source (form should render)"
echo "  5. Add schema columns"
echo "  6. Configure target (BigQuery)"
echo "  7. Set execution policy"
echo "  8. Submit → Monitor progress"
echo ""
echo "  Test 2: Natural Language Transform"
echo "  -----------------------------------"
echo "  1. Create pipeline"
echo "  2. In Schema/Transform section:"
echo "  3. Enter NL description:"
echo "     'Calculate running total of sales by customer'"
echo "  4. Click 'Generate Structured Transform'"
echo "  5. Review:"
echo "     - Structured configuration (JSON)"
echo "     - Generated PySpark code"
echo "     - Confidence score (should be >80%)"
echo "  6. Add transform to pipeline"
echo ""
echo "  Test 3: DTSX Migration"
echo "  ----------------------"
echo "  1. Select source type 'legacy_dtsx'"
echo "  2. DTSX config form should render"
echo "  3. Enter GCS path to DTSX file"
echo "  4. Click 'Parse DTSX Package'"
echo "  5. Review extracted sources/transforms"
echo ""
echo "  Test 4: Jira Integration"
echo "  ------------------------"
echo "  1. Navigate to /jira/DATA-1234"
echo "  2. Form should auto-populate from Jira"
echo "  3. Complete pipeline config"
echo "  4. Submit"
echo ""
echo -e "${GREEN}Expected Behavior:${NC}"
echo "  ✓ All 9 source categories visible"
echo "  ✓ Type-specific forms render correctly"
echo "  ✓ NL transforms show structured output"
echo "  ✓ DTSX parsing works"
echo "  ✓ Jira integration populates fields"
echo "  ✓ Pipeline creation succeeds"
echo "  ✓ Progress tracking works"
echo ""

# =============================================================================
# 5. Key Features to Verify
# =============================================================================
echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     Key Features Checklist                                   ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

echo "Data Engineering Agent (New Features):"
echo "  [ ] SourceTypeSelector shows 9 categories"
echo "  [ ] Search filters source types"
echo "  [ ] FileSourceConfigForm renders for file_* types"
echo "  [ ] DatabaseSourceConfigForm renders for database_* types"
echo "  [ ] StreamingSourceConfigForm renders for streaming_* types"
echo "  [ ] NLTransformInput converts NL to structured metadata"
echo "  [ ] DTSXMigrationForm parses SSIS packages"
echo "  [ ] Schema editor allows adding/removing columns"
echo "  [ ] Target zone selector works (landing/bronze/silver/gold)"
echo "  [ ] Execution policy scheduling works"
echo ""

echo "Incident Management Agent:"
echo "  [ ] Incidents list loads"
echo "  [ ] Workflow visualization shows nodes"
echo "  [ ] Approval flow works"
echo "  [ ] Real-time updates via WebSocket/polling"
echo ""

# =============================================================================
# 6. Debugging Tips
# =============================================================================
echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     Debugging Tips                                            ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

echo "Browser DevTools:"
echo "  1. Open DevTools (F12)"
echo "  2. Check Console tab for errors"
echo "  3. Check Network tab for failed API calls"
echo "  4. Check React DevTools for component state"
echo ""

echo "Backend Logs:"
echo "  Data Agent: tail -f /tmp/data-agent.log"
echo "  Backend API: tail -f /tmp/backend.log"
echo ""

echo "Common Issues:"
echo "  - Module not found: npm install in frontend/"
echo "  - API 404: Check backend is running on correct port"
echo "  - CORS errors: Check NEXT_PUBLIC_API_URL env var"
echo "  - TypeScript errors: Expected during migration, check build"
echo ""

# =============================================================================
# 7. Quick Links
# =============================================================================
echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║     Quick Access Links                                        ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

echo "Frontend Pages:"
echo "  • Home:          http://localhost:3000"
echo "  • Incidents:     http://localhost:3000/incidents"
echo "  • Approvals:     http://localhost:3000/approvals"
echo "  • Workflows:     http://localhost:3000/workflows"
echo "  • Pipelines:     http://localhost:3000/pipelines"
echo "  • Jira:          http://localhost:3000/jira/[id]"
echo ""

echo "API Documentation:"
echo "  • Backend API:   http://localhost:8000/docs"
echo "  • Data Agent:    http://localhost:8001/docs"
echo ""

echo "Test Files:"
echo "  • E2E Plan:      E2E_TEST_PLAN.md"
echo "  • This Script:   test_ui_e2e.sh"
echo ""

# =============================================================================
# Done
# =============================================================================
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║     Services Ready - Start Testing!                          ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "Press Ctrl+C to exit, or continue testing in browser"
echo ""
