#!/bin/bash
# Quick DynamoDB Cache Clearing Script for Teddy Data Pipeline
# Simple wrapper around the Python cache cleaner

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/clear_dynamodb_cache.py"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
print_header() {
    echo -e "${BLUE}🧹 Teddy Data Pipeline - DynamoDB Cache Cleaner${NC}"
    echo "=================================================="
}

print_usage() {
    echo "Usage: $0 [OPTION] [ENVIRONMENT]"
    echo ""
    echo "Options:"
    echo "  stats      Show cache statistics"
    echo "  all        Clear all cache data"
    echo "  parcel     Clear parcel cache only"
    echo "  rate       Clear rate limit data"
    echo "  metadata   Clear API metadata"
    echo "  expired    Clear expired items only"
    echo ""
    echo "Environment (optional):"
    echo "  dev        Development environment (default)"
    echo "  prod       Production environment"
    echo ""
    echo "Examples:"
    echo "  $0 stats                # Show dev cache stats"
    echo "  $0 all dev              # Clear all dev cache"
    echo "  $0 parcel prod          # Clear prod parcel cache"
    echo "  $0 expired              # Clear expired items in dev"
}

# Check if Python script exists
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo -e "${RED}❌ Error: Python script not found at $PYTHON_SCRIPT${NC}"
    exit 1
fi

# Parse arguments
OPERATION=""
ENVIRONMENT="dev"

if [ $# -eq 0 ]; then
    print_header
    print_usage
    exit 1
fi

OPERATION="$1"
if [ $# -eq 2 ]; then
    ENVIRONMENT="$2"
fi

# Validate environment
if [ "$ENVIRONMENT" != "dev" ] && [ "$ENVIRONMENT" != "prod" ]; then
    echo -e "${RED}❌ Invalid environment: $ENVIRONMENT${NC}"
    echo -e "${YELLOW}Valid environments: dev, prod${NC}"
    exit 1
fi

print_header
echo -e "${BLUE}Environment: ${ENVIRONMENT}${NC}"
echo ""

# Execute operations
case "$OPERATION" in
    "stats")
        echo -e "${GREEN}📊 Showing cache statistics...${NC}"
        python3 "$PYTHON_SCRIPT" --env "$ENVIRONMENT" --stats
        ;;
    
    "all")
        echo -e "${YELLOW}⚠️  Clearing ALL cache data...${NC}"
        python3 "$PYTHON_SCRIPT" --env "$ENVIRONMENT" --all
        ;;
    
    "parcel")
        echo -e "${GREEN}🗂️  Clearing parcel cache...${NC}"
        python3 "$PYTHON_SCRIPT" --env "$ENVIRONMENT" --parcel
        ;;
    
    "rate")
        echo -e "${GREEN}⏱️  Clearing rate limit data...${NC}"
        python3 "$PYTHON_SCRIPT" --env "$ENVIRONMENT" --rate-limit
        ;;
    
    "metadata")
        echo -e "${GREEN}📋 Clearing API metadata...${NC}"
        python3 "$PYTHON_SCRIPT" --env "$ENVIRONMENT" --metadata
        ;;
    
    "expired")
        echo -e "${GREEN}🕐 Clearing expired items...${NC}"
        python3 "$PYTHON_SCRIPT" --env "$ENVIRONMENT" --expired
        ;;
    
    "help"|"--help"|"-h")
        print_usage
        ;;
    
    *)
        echo -e "${RED}❌ Unknown operation: $OPERATION${NC}"
        echo ""
        print_usage
        exit 1
        ;;
esac

echo ""
echo -e "${BLUE}✅ Cache operation completed!${NC}"