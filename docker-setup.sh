#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🐳 Knesset OData Explorer - Docker Setup${NC}"
echo "=========================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed. Please install Docker first.${NC}"
    echo "Visit: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo -e "${RED}❌ Docker Compose is not available. Please install Docker Compose.${NC}"
    exit 1
fi

# Determine docker compose command
COMPOSE_CMD="docker-compose"
if docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
fi

echo -e "${GREEN}✅ Docker and Docker Compose are available${NC}"

# Create necessary directories
echo -e "${YELLOW}📁 Creating necessary directories...${NC}"
mkdir -p data logs backups
chmod 755 data logs backups

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo -e "${YELLOW}📝 Creating .env file...${NC}"
    cat > .env << EOF
# Database Configuration
DATABASE_PATH=/app/data/warehouse.duckdb
PARQUET_PATH=/app/data/parquet

# API Configuration  
KNESSET_API_BASE_URL=http://knesset.gov.il/Odata/ParliamentInfo.svc
API_TIMEOUT=30
MAX_RETRIES=3

# Streamlit Configuration
STREAMLIT_CACHE_DISABLED=1
STREAMLIT_HOST=0.0.0.0
STREAMLIT_PORT=8501

# Development Settings
PYTHONPATH=/app/src
LOG_LEVEL=INFO
EOF
    echo -e "${GREEN}✅ Created .env file${NC}"
fi

# Function to show usage
show_usage() {
    echo -e "\n${BLUE}🚀 Usage Options:${NC}"
    echo "1. Production mode:     $COMPOSE_CMD up -d knesset-app"
    echo "2. Development mode:    $COMPOSE_CMD up -d knesset-dev"
    echo "3. With backup service: $COMPOSE_CMD --profile backup up -d"
    echo "4. Build only:          $COMPOSE_CMD build"
    echo "5. View logs:           $COMPOSE_CMD logs -f"
    echo "6. Stop all:            $COMPOSE_CMD down"
    echo ""
    echo -e "${BLUE}🔗 Access URLs:${NC}"
    echo "- Production app: http://localhost:8501"
    echo "- Development app: http://localhost:8502"
    echo ""
    echo -e "${BLUE}🛠️  Development Commands:${NC}"
    echo "- Shell access: $COMPOSE_CMD exec knesset-dev bash"
    echo "- Run tests: $COMPOSE_CMD exec knesset-dev pytest"
    echo "- Fetch data: $COMPOSE_CMD exec knesset-dev python -m backend.fetch_table --table KNS_Person"
}

# Parse command line arguments
case "${1:-}" in
    "build")
        echo -e "${YELLOW}🏗️  Building Docker images...${NC}"
        $COMPOSE_CMD build
        echo -e "${GREEN}✅ Build completed${NC}"
        ;;
    "up"|"start")
        mode="${2:-production}"
        if [ "$mode" = "dev" ] || [ "$mode" = "development" ]; then
            echo -e "${YELLOW}🚀 Starting in development mode...${NC}"
            $COMPOSE_CMD up -d knesset-dev
            echo -e "${GREEN}✅ Development container started${NC}"
            echo -e "${BLUE}📱 Access development app at: http://localhost:8502${NC}"
            echo -e "${BLUE}🔧 Get shell access: $COMPOSE_CMD exec knesset-dev bash${NC}"
        else
            echo -e "${YELLOW}🚀 Starting in production mode...${NC}"
            $COMPOSE_CMD up -d knesset-app
            echo -e "${GREEN}✅ Production container started${NC}"
            echo -e "${BLUE}📱 Access app at: http://localhost:8501${NC}"
        fi
        ;;
    "stop"|"down")
        echo -e "${YELLOW}🛑 Stopping all containers...${NC}"
        $COMPOSE_CMD down
        echo -e "${GREEN}✅ All containers stopped${NC}"
        ;;
    "logs")
        service="${2:-}"
        if [ -n "$service" ]; then
            $COMPOSE_CMD logs -f "$service"
        else
            $COMPOSE_CMD logs -f
        fi
        ;;
    "shell"|"bash")
        service="${2:-knesset-dev}"
        echo -e "${YELLOW}🐚 Opening shell in $service...${NC}"
        $COMPOSE_CMD exec "$service" bash
        ;;
    "test")
        echo -e "${YELLOW}🧪 Running tests...${NC}"
        $COMPOSE_CMD exec knesset-dev pytest
        ;;
    "setup")
        echo -e "${YELLOW}⚙️  Initial setup with sample data...${NC}"
        $COMPOSE_CMD up -d knesset-dev
        echo "Waiting for container to be ready..."
        sleep 10
        echo "Downloading sample data..."
        $COMPOSE_CMD exec knesset-dev python -m backend.fetch_table --table KNS_Person
        $COMPOSE_CMD exec knesset-dev python -m backend.fetch_table --table KNS_Query
        echo -e "${GREEN}✅ Setup completed with sample data${NC}"
        ;;
    "clean")
        echo -e "${YELLOW}🧹 Cleaning up containers and images...${NC}"
        $COMPOSE_CMD down --rmi all --volumes --remove-orphans
        echo -e "${GREEN}✅ Cleanup completed${NC}"
        ;;
    "help"|"-h"|"--help"|"")
        show_usage
        ;;
    *)
        echo -e "${RED}❌ Unknown command: $1${NC}"
        show_usage
        exit 1
        ;;
esac

if [ "${1:-}" != "help" ] && [ "${1:-}" != "-h" ] && [ "${1:-}" != "--help" ] && [ "${1:-}" != "" ]; then
    echo ""
    echo -e "${BLUE}💡 Tip: Run '$0 help' to see all available commands${NC}"
fi