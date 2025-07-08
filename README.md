# FXHash Token Analysis Tool

A Python-based tool for analyzing token holders and transfers on the Base network. This tool provides detailed insights into token distribution, holder statistics, and transfer history.

## Project Structure

```
fxhash/
├── README.md                 # This file
├── .gitignore               # Git ignore rules
├── scripts/                 # Python analysis scripts
│   ├── snapshot_holders.py  # Main holder analysis script
│   ├── all_holdersv2.py    # Comprehensive holder analysis
│   ├── analyze_holders.py  # Transfer data analysis
│   └── OLD_get_holders.py  # Legacy script
├── data/                    # Data files
│   ├── csv_outputs/        # CSV output files
│   │   ├── current_holders.csv
│   │   ├── all_current_holders.csv
│   │   ├── token_holders.csv
│   │   ├── daily_statistics.csv
│   │   └── timestamped holder files
│   └── processed/          # Processed data files
│       └── transfer_data.pkl
└── outputs/                # Future analysis outputs
```

## Features

- Automated token holder analysis
- Real-time balance tracking
- Transfer event processing
- CSV export of holder data
- Key metrics calculation (total holders, supply, average balance)
- Top holder identification
- Daily transfer statistics
- Comprehensive holder snapshots

## Prerequisites

- Python 3.x
- pip (Python package installer)
- Internet connection for Base network access

## Installation

1. Clone this repository:
```bash
git clone https://github.com/qaulv/fxhash.git
cd fxhash
```

2. The scripts will automatically install required dependencies:
- web3
- pandas
- tqdm

## Usage

### Main Token Holder Analysis

Run the primary analysis script:
```bash
python scripts/snapshot_holders.py
```

This will:
1. Connect to the Base network
2. Process all transfer events from the token's launch block
3. Calculate current holder balances
4. Generate a timestamped CSV file in `data/csv_outputs/`
5. Display key metrics and top 10 holders

### Comprehensive Holder Analysis

For a more thorough analysis with retry logic:
```bash
python scripts/all_holdersv2.py
```

### Analyze Transfer Data

To analyze previously collected transfer data:
```bash
python scripts/analyze_holders.py
```

Note: This requires `transfer_data.pkl` to exist in `data/processed/`

## Output Files

### CSV Files (data/csv_outputs/)
- `YYYY-MM-DD_HHMM_current_holders.csv` - Timestamped holder snapshots
- `current_holders.csv` - Latest holder analysis
- `all_current_holders.csv` - Comprehensive holder list
- `daily_statistics.csv` - Daily transfer volume and count statistics
- `token_holders.csv` - Historical holder data

### Processed Data (data/processed/)
- `transfer_data.pkl` - Serialized transfer event data

## Configuration

The scripts use the following default settings:
- Network: Base Mainnet
- Contract Address: 0x5Fc2843838e65eb0B5d33654628F446d54602791
- Token Decimals: 18
- Starting Block: 23036627

To modify these settings, edit the constants in the respective scripts:
```python
RPC_URL = 'https://mainnet.base.org'
CONTRACT_ADDRESS = '0x5Fc2843838e65eb0B5d33654628F446d54602791'
DECIMALS = 18
```

## Performance

- Scripts process blocks in chunks (1000-2000) for optimal performance
- Progress is displayed using progress bars
- Large datasets are handled efficiently using pandas
- Retry logic included for network reliability

## Error Handling

The scripts include robust error handling for:
- Network connectivity issues
- Block processing errors
- Package installation failures
- Data serialization/deserialization

## Script Descriptions

### `snapshot_holders.py`
Main analysis script with automatic package installation and user-friendly output. Generates timestamped CSV files and comprehensive metrics.

### `all_holdersv2.py`
Comprehensive holder analysis with smaller chunk sizes and retry logic for maximum reliability.

### `analyze_holders.py`
Analyzes pre-collected transfer data from pickle files. Generates daily statistics and holder concentration metrics.

### `OLD_get_holders.py`
Legacy script maintained for reference.

## Contributing

Feel free to submit issues and enhancement requests!

## License

[Add your license information here] 