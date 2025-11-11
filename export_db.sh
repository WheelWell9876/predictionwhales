#!/bin/bash

# Polymarket Terminal Data Export and Analysis Script

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
DB_PATH="polymarket_terminal.db"
EXPORT_DIR="backend/data/tables_ex"
LIMIT=100

# Function to display help
show_help() {
    echo -e "${BLUE}Polymarket Terminal Data Export & Analysis Tool${NC}"
    echo ""
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  export       Export all tables to JSON files"
    echo "  analyze      Generate analysis report"
    echo "  both         Run both export and analysis"
    echo "  clean        Clean export directory"
    echo ""
    echo "Options:"
    echo "  --db PATH    Database path (default: polymarket_terminal.db)"
    echo "  --output DIR Output directory for export (default: backend/data/tables_ex)"
    echo "  --limit N    Number of rows to export per table (default: 100)"
    echo "  --tables     Specific tables to export (space-separated)"
    echo ""
    echo "Examples:"
    echo "  $0 export                    # Export all tables with defaults"
    echo "  $0 export --limit 500        # Export 500 rows per table"
    echo "  $0 export --tables events markets users  # Export specific tables"
    echo "  $0 analyze                   # Generate analysis report"
    echo "  $0 both                      # Export and analyze"
}

# Function to check if database exists
check_database() {
    if [ ! -f "$DB_PATH" ]; then
        echo -e "${RED}Error: Database not found at $DB_PATH${NC}"
        echo "Please run 'python setup.py --initial-load' first"
        exit 1
    fi
}

# Function to export data
export_data() {
    echo -e "${BLUE}Starting data export...${NC}"
    check_database

    # Create export directory if it doesn't exist
    mkdir -p "$EXPORT_DIR"

    # Build command
    CMD="python export_data.py --db $DB_PATH --output $EXPORT_DIR --limit $LIMIT"

    # Add tables if specified
    if [ ! -z "$TABLES" ]; then
        CMD="$CMD --tables $TABLES"
    fi

    # Run export
    $CMD

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Export completed successfully!${NC}"
        echo -e "Files saved to: ${BLUE}$EXPORT_DIR${NC}"

        # Show file count
        FILE_COUNT=$(ls -1 "$EXPORT_DIR"/*.json 2>/dev/null | wc -l)
        echo -e "Total files created: ${YELLOW}$FILE_COUNT${NC}"
    else
        echo -e "${RED}❌ Export failed!${NC}"
        exit 1
    fi
}

# Function to analyze data
analyze_data() {
    echo -e "${BLUE}Starting data analysis...${NC}"
    check_database

    # Create output directory
    mkdir -p "backend/data"

    # Run analysis
    python analyze_data.py --db "$DB_PATH" --output "backend/data/analysis_report.json"

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Analysis completed successfully!${NC}"
        echo -e "Report saved to: ${BLUE}backend/data/analysis_report.json${NC}"
    else
        echo -e "${RED}❌ Analysis failed!${NC}"
        exit 1
    fi
}

# Function to clean export directory
clean_export() {
    echo -e "${YELLOW}Cleaning export directory...${NC}"

    if [ -d "$EXPORT_DIR" ]; then
        rm -rf "$EXPORT_DIR"/*
        echo -e "${GREEN}✅ Export directory cleaned${NC}"
    else
        echo -e "${BLUE}Export directory doesn't exist${NC}"
    fi
}

# Parse command line arguments
COMMAND=$1
shift

# Parse options
while [[ $# -gt 0 ]]; do
    case $1 in
        --db)
            DB_PATH="$2"
            shift 2
            ;;
        --output)
            EXPORT_DIR="$2"
            shift 2
            ;;
        --limit)
            LIMIT="$2"
            shift 2
            ;;
        --tables)
            shift
            TABLES="$@"
            break
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Execute command
case $COMMAND in
    export)
        export_data
        ;;
    analyze)
        analyze_data
        ;;
    both)
        export_data
        echo ""
        analyze_data
        ;;
    clean)
        clean_export
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo -e "${RED}Invalid command: $COMMAND${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac