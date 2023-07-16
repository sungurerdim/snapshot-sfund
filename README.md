# snapshot-sfund
A Python script that takes snapshots of SFUND stakers/farmers

***Having an .env file is necessary to run the script. You can find an example below.***

### Usage
```
python main.py [-h] [-e ENV] [-i INCLUDE] [-nc] [-st START_TIME] [-et END_TIME] [-d DATA_DIR] [-o OUTPUT_DIR]
               [-of OUTPUT_FILENAME]

options:
  -h, --help            show this help message and exit
  -e ENV, --env ENV     Specify env variables file (default: .env)
  -i INCLUDE, --include INCLUDE
                        Set snapshot inclusion period in days (default: 90)
  -nc, --no-color       Disable color output (default: not set)
  -st START_TIME, --start-time START_TIME
                        Set start time epoch (default: 90 days before the END_TIME parameter)
  -et END_TIME, --end-time END_TIME
                        Set end time epoch (default: current UTC epoch time)
  -d DATA_DIR, --data-dir DATA_DIR
                        Path to save transaction exports of contracts as CSV files (default: Data)
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Path to save snapshot CSV file (default: Output)
  -of OUTPUT_FILENAME, --output-filename OUTPUT_FILENAME
                        Preferred name for snapshot CSV file (default: Snapshot.csv)
```

### Example .env file
```
API_ENDPOINT='https://api.bscscan.com/api'
API_KEY='Your_BSC_Scan_API_Key'
TIERS='9,8,7,6,5,4,3,2,1'
TIER_LIMITS='100000,50000,25000,10000,7500,5000,2500,1000,250'
POOL_WEIGHTS='325,150,70,26,19,12,5.8,2.2,1.1'
EXCLUDE='0x0000000000000000000000000000000000000000,0x000000000000000000000000000000000000dead'
```