import sys
import json
import os
import logging
from datetime import datetime

# Required keys in config.json
REQUIRED_CONFIG_KEYS = ['host', 'user', 'password', 'database', 'fieldDetailsCsvPath']

def setup_logger():
    log_file = f"logs/script_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Logger initialized.")
    return log_file

def load_config(config_file):
    if not os.path.exists(config_file):
        logging.error(f"Config file not found: {config_file}")
        sys.exit(1)

    with open(config_file, 'r') as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse config file: {e}")
            sys.exit(1)

    missing_keys = [key for key in REQUIRED_CONFIG_KEYS if key not in config]
    if missing_keys:
        logging.error(f"Missing config keys: {', '.join(missing_keys)}")
        sys.exit(1)

    logging.info("Config file loaded and validated.")
    return config

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <config_file.json>")
        sys.exit(1)

    log_file = setup_logger()
    config_file = sys.argv[1]
    logging.info(f"Using config file: {config_file}")

    config = load_config(config_file)

    logging.info("Config loaded successfully.")
    logging.info("Proceeding to the next step... (e.g., connecting to DB, loading CSV, etc.)")

if __name__ == "__main__":
    main()
