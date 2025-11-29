#!/bin/bash
#
# PowerGrid AI - One-Click Execution Script
#
# Usage:
#   ./run.sh              - Run full pipeline
#   ./run.sh --regenerate - Regenerate data and run
#   ./run.sh --skip-data  - Skip data generation
#   ./run.sh --help       - Show help
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo -e "${BLUE}"
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                                                               ║"
echo "║   ⚡ PowerGrid AI - Distributed Energy Intelligence          ║"
echo "║                                                               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Check Java version and set JAVA_HOME - Spark requires Java 11 or 17
setup_java() {
    # First, check Homebrew locations for Java 17 (most reliable)
    if [[ -d "/opt/homebrew/opt/openjdk@17" ]]; then
        export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
        export PATH="$JAVA_HOME/bin:$PATH"
        echo -e "${GREEN}Using Java 17 from Homebrew: $JAVA_HOME${NC}"
        return 0
    elif [[ -d "/usr/local/opt/openjdk@17" ]]; then
        export JAVA_HOME="/usr/local/opt/openjdk@17"
        export PATH="$JAVA_HOME/bin:$PATH"
        echo -e "${GREEN}Using Java 17 from Homebrew: $JAVA_HOME${NC}"
        return 0
    fi

    # Try java_home utility as fallback
    if command -v /usr/libexec/java_home &> /dev/null; then
        JAVA17_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null || echo "")
        if [[ -n "$JAVA17_HOME" && ! "$JAVA17_HOME" =~ "25" ]]; then
            export JAVA_HOME="$JAVA17_HOME"
            export PATH="$JAVA_HOME/bin:$PATH"
            echo -e "${GREEN}Using Java 17 from: $JAVA_HOME${NC}"
            return 0
        fi

        JAVA11_HOME=$(/usr/libexec/java_home -v 11 2>/dev/null || echo "")
        if [[ -n "$JAVA11_HOME" ]]; then
            export JAVA_HOME="$JAVA11_HOME"
            export PATH="$JAVA_HOME/bin:$PATH"
            echo -e "${GREEN}Using Java 11 from: $JAVA_HOME${NC}"
            return 0
        fi
    fi

    # Check current java version
    if command -v java &> /dev/null; then
        JAVA_VER=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
        if [[ "$JAVA_VER" -ge 20 ]]; then
            echo -e "${RED}Error: Java $JAVA_VER detected. Apache Spark requires Java 11 or 17.${NC}"
            echo -e "${YELLOW}"
            echo "Your Java version ($JAVA_VER) is too new for Spark."
            echo ""
            echo "Please install Java 17 using Homebrew:"
            echo "    brew install openjdk@17"
            echo -e "${NC}"
            exit 1
        fi
    else
        echo -e "${RED}Error: Java is not installed. Please install Java 11 or 17.${NC}"
        exit 1
    fi
}

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is not installed.${NC}"
    exit 1
fi

# Setup Java (auto-detect Java 17 or 11)
setup_java

# Check if virtual environment exists
if [ -d "venv" ]; then
    echo -e "${GREEN}Activating virtual environment...${NC}"
    source venv/bin/activate
else
    echo -e "${YELLOW}Virtual environment not found.${NC}"
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    python3 -m venv venv
    source venv/bin/activate

    echo -e "${GREEN}Installing dependencies...${NC}"
    pip install --upgrade pip
    pip install -r requirements.txt
fi

# Check dependencies
echo -e "${BLUE}Checking dependencies...${NC}"
python3 -c "import pyspark; import pandas; import numpy; import faker; import matplotlib" 2>/dev/null || {
    echo -e "${YELLOW}Installing missing dependencies...${NC}"
    pip install -r requirements.txt
}

# Java 17+ compatibility fix for Spark
# This fixes the "getSubject is not supported" error in newer Java versions
export JAVA_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
-Djava.security.manager=allow"

export _JAVA_OPTIONS="$JAVA_OPTS"
export SPARK_SUBMIT_OPTS="$JAVA_OPTS"

# Run the main application
echo -e "${GREEN}Starting PowerGrid AI...${NC}"
echo ""

python3 -m src.main "$@"

echo ""
echo -e "${GREEN}Done!${NC}"
