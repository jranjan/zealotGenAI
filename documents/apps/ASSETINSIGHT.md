# Asset Insight Studio

A comprehensive asset management and analysis platform built with Streamlit, designed for analyzing and visualizing asset ownership, cloud infrastructure, and organizational data.

## Overview

Asset Insight Studio is a powerful web application that provides comprehensive asset management and analysis capabilities. The platform offers source data analysis with support for multiple asset classes, high-performance data transformation using multiprocessing, and deep ownership analytics including cloud distribution and team assignments. Key features include:

- **Source Data Analysis**: Scan and analyze asset directories with support for multiple asset classes
- **Data Transformation**: Flatten and normalize complex JSON asset data using high-performance multiprocessing
- **Ownership Analytics**: Deep dive into asset ownership patterns, cloud distribution, and team assignments
- **Interactive Dashboards**: Rich visualizations with charts, tables, and real-time metrics
- **Scalable Processing**: Handle millions of assets with optimized DuckDB and multiprocessing
- **High-Performance Processing**: Multiprocessing support for large datasets
- **Interactive Analytics**: Real-time charts and visualizations
- **Smart Data Flattening**: Automatic extraction of nested JSON structures
- **Ownership Intelligence**: Comprehensive ownership analysis and reporting
- **Efficient Storage**: DuckDB-based analytics database
- **Modern UI**: Clean, responsive Streamlit interface

## Code

### Folder Structure

```
zealot/apps/assetinsight/
├── analyser/                    # Analysis engines
│   ├── asset.py                # Abstract base class for asset analysis
│   ├── owner.py                # Ownership analysis implementation
│   ├── security.py             # Security analysis (placeholder)
│   └── network.py              # Network analysis (placeholder)
├── common/                     # Shared utilities and constants
│   ├── asset_class.py          # Asset class enumeration
│   ├── system_data.py          # System directory constants
│   └── analyser_factory.py     # Factory for creating analysers
├── config/                     # Configuration files
│   └── asset.yaml              # Asset class field definitions
├── dashboard/                  # Streamlit UI components
│   ├── tabs/                   # Main application tabs
│   │   ├── source.py           # Source data selection and analysis
│   │   ├── normaliser.py       # Data transformation and normalization
│   │   ├── analysis/           # Analysis-specific tabs
│   │   │   └── ownership.py    # Ownership analysis dashboard
│   │   └── base.py             # Base tab class
│   └── app.py                  # Main Streamlit application
├── database/                   # Data storage and retrieval
│   └── reader/                 # Database readers
│       ├── base.py             # Abstract reader interface
│       ├── duckdb.py           # DuckDB reader implementation
│       ├── duckdb_sonic.py     # High-performance multiprocessing reader
│       └── factory.py          # Reader factory
├── scanner/                    # Data scanning utilities
│   ├── base.py                 # Abstract scanner interface
│   ├── source_scanner.py       # Source data scanner
│   ├── sonic_scanner.py        # Multithreaded scanner
│   └── supersonic_scanner.py   # Multiprocessing scanner
├── transformer/                # Data transformation engines
│   ├── flattener.py            # Base flattener class
│   ├── flattener_sonic.py      # Multithreaded flattener
│   ├── flattener_supersonic.py # Multiprocessing flattener
│   ├── factory.py              # Transformer factory
│   └── utils/                  # Transformation utilities
│       └── flattener_helper.py # Flattening helper functions
├── utils/                      # Application utilities
│   └── dataframe_utils.py      # PyArrow-compatible DataFrame utilities
├── requirements.txt            # Python dependencies
└── install_dependencies.sh     # Dependency installation script
```

### Package Description

#### Core Components

| Package                          | Description                                | Key Features                                                                 |
|:---------------------------------|:-------------------------------------------|:-----------------------------------------------------------------------------|
| **Analyser** (`analyser/`)       | Analysis engines for different asset types | Abstract base classes, SQL-based queries, extensible architecture            |
| **Dashboard** (`dashboard/`)     | Streamlit-based web interface              | Modular tab system, real-time visualization, interactive components          |
| **Database** (`database/`)       | Data storage and retrieval system          | DuckDB analytics database, multiprocessing data loading, SQL-centric queries |
| **Scanner** (`scanner/`)         | File system scanning utilities             | Multi-asset class support, optimized for large directories                   |
| **Transformer** (`transformer/`) | Data transformation engines                | JSON flattening, multiprocessing support, configurable field extraction      |

#### Key Technologies

| Technology    | Purpose                           |
|:--------------|:----------------------------------|
| **Streamlit** | Web application framework         |
| **DuckDB**    | In-process SQL analytics database |
| **Pandas**    | Data manipulation and analysis    |
| **Plotly**    | Interactive visualizations        |
| **PyArrow**   | High-performance data processing  |
| **orjson**    | Fast JSON parsing                 |
| **psutil**    | System monitoring                 |

## Getting Started

### Prerequisites

- Python 3.8 or higher
- pip package manager
- Git (for cloning the repository)

### Creating Virtual Environment

1. **Navigate to the project directory:**
   ```bash
   cd zealot/apps/assetinsight
   ```

2. **Create a virtual environment:**
   ```bash
   python3 -m venv venv
   ```

3. **Activate the virtual environment:**
   
   **On macOS/Linux:**
   ```bash
   source venv/bin/activate
   ```
   
   **On Windows:**
   ```bash
   venv\Scripts\activate
   ```

4. **Install dependencies:**
   ```bash
   pip3 install -r requirements.txt
   ```

### Running the App

1. **Ensure the virtual environment is activated:**
   ```bash
   source venv/bin/activate  # macOS/Linux
   # or
   venv\Scripts\activate     # Windows
   ```

2. **Run the Streamlit application:**
   ```bash
   streamlit run app.py
   ```

3. **Access the application:**
   - Open your web browser
   - Navigate to `http://localhost:8501`
   - The Asset Insight Studio interface will load




**Asset Insight Studio** - Empowering data-driven asset management decisions through intelligent analysis and visualization.
