import sys
import json
import os
import logging
import mysql.connector
import time
import csv
from datetime import datetime
from collections import defaultdict

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

def load_field_details(config):
    path = config['fieldDetailsCsvPath']
    if not os.path.exists(path):
        logging.error(f"Field details CSV file not found: {path}")
        sys.exit(1)

    with open(path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return [row for row in reader]

def connect_to_db(config):
    try:
        connection = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )
        logging.info("Database connection established.")
        return connection
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to database: {err}")
        sys.exit(1)

def fetch_records_in_batches(connection, base_query, batch_size=100):
    offset = 0
    while True:
        paginated_query = f"{base_query} LIMIT {batch_size} OFFSET {offset}"
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(paginated_query)
            records = cursor.fetchall()
            if not records:
                logging.info("No more records to fetch.")
                break
            logging.info(f"Fetched {len(records)} records from offset {offset}.")
            yield records
        offset += batch_size

def fetch_form_ctxt_ids(connection, cp_ids):
    cp_ids = list(set(cp_ids))
    cp_ids_str = ', '.join(map(str, cp_ids))
    query = f"SELECT identifier, cp_id FROM catissue_form_context WHERE cp_id IN ({cp_ids_str})"
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    return {str(row['cp_id']): row['identifier'] for row in rows}

def divide_records(records):
    to_update = []
    to_insert = []

    for record in records:
        if record.get('custom_field_record_id'):
            to_update.append(record)
        else:
            to_insert.append(record)

    logging.info(f"Records divided: {len(to_update)} to update, {len(to_insert)} to insert.")
    return to_update, to_insert

def insert_records(connection, records, form_ctxt_map, field_details, failed_log_file='failed_inserts.csv'):
    cursor = connection.cursor()
    cursor.execute('SELECT MAX(record_id) FROM catissue_form_record_entry')
    record_id = cursor.fetchone()[0] or 1

    with open(failed_log_file, mode='w', newline='', encoding='utf-8') as failed_log:
        failed_writer = csv.writer(failed_log)
        failed_writer.writerow(['Record', 'Error'])

        success_count = 0
        failure_count = 0
        batch_size = 100
        current_batch = []
        batch_record_ids = []
        total_inserted = 0
        batch_start_time = time.time()

        def update_sequence():
            update_seq_sql = '''
                UPDATE dyextn_id_seq
                SET LAST_ID = (SELECT MAX(record_id) + 1 FROM catissue_form_record_entry)
                WHERE TABLE_NAME = 'RECORD_ID_SEQ'
            '''
            cursor.execute(update_seq_sql)
            connection.commit()

        # Create field mapping from field_details
        field_mappings = []
        for row in field_details:
            field_mappings.append({
                'legacy_field': row['Legacy Field Name'].strip(),
                'target_field': row['Target Field Name'].strip(),
                'target_table': row['Target Table Name'].strip(),
                'target_column': row['Target Column Name'].strip(),
                'is_multiselect': row['Is Multi-Select'].strip().lower() == 'yes'
            })

        for record in records:
            record_id += 1
            try:
                cp_id = record['cp_id']
                form_ctxt_id = form_ctxt_map[str(cp_id)]

                # Main record entry SQL
                entry_sql = '''
                    INSERT INTO catissue_form_record_entry 
                    (FORM_CTXT_ID, OBJECT_ID, RECORD_ID, UPDATED_BY, ACTIVITY_STATUS, FORM_STATUS)
                    VALUES (%s, %s, %s, %s, %s, %s)
                '''
                entry_params = (
                    form_ctxt_id,
                    record['specimen_id'],
                    record_id,
                    2,
                    'Active',
                    'COMPLETE'
                )
                current_batch.append((entry_sql, entry_params))
                batch_record_ids.append(record_id)

                for mapping in field_mappings:
                    form_field = 'form_' + mapping['legacy_field'].lower().replace(' ', '_')
                    if form_field not in record:
                        logging.warning(f"Form field {form_field} not found in record")
                        continue

                    field_value = record[form_field]
                    if field_value in (None, 'NULL', '') or str(field_value).strip() == '':
                        continue

                    field_value = str(field_value).strip()

                    if mapping['is_multiselect']:
                        values = [v.strip() for v in field_value.split(',')] if ',' in field_value else [field_value]
                        for val in values:
                            if not val:
                                continue
                            insert_sql = f'INSERT INTO {mapping["target_table"]} (VALUE, RECORD_ID) VALUES (%s, %s)'
                            insert_params = [val, record_id]
                            current_batch.append((insert_sql, insert_params))
                            logging.info(f"Inserting multi-select value: {val}")
                    else:
                        insert_sql = f'INSERT INTO {mapping["target_table"]} (IDENTIFIER, {mapping["target_column"]}) VALUES (%s, %s)'
                        insert_params = [record_id, field_value]
                        current_batch.append((insert_sql, insert_params))
                        logging.info(f"Inserting value: {field_value} into {mapping['target_table']}")

                if len(current_batch) >= batch_size:
                    try:
                        if connection.in_transaction:
                            connection.rollback()
                        connection.start_transaction()

                        for i, (sql, params) in enumerate(current_batch, 1):
                            try:
                                cursor.execute(sql, params)
                            except Exception as e:
                                logging.error(f"Failed to execute statement {i}: {sql} with values {params}")
                                raise

                        connection.commit()
                        success_count += len(batch_record_ids)
                        total_inserted += len(batch_record_ids)
                        time_taken = time.time() - batch_start_time
                        logging.info(f"✅ Total inserted: {total_inserted} | 🟢 Success: {success_count} | 🔴 Failed: {failure_count} | ⏱️ Time taken: {time_taken:.2f}s")
                        update_sequence()
                    except Exception as e:
                        connection.rollback()
                        error_msg = f"Batch failed: {str(e)}"
                        logging.error(error_msg)
                        for record_id in batch_record_ids:
                            failed_writer.writerow([f"Record ID {record_id}", error_msg])
                        failure_count += len(batch_record_ids)

                    current_batch = []
                    batch_record_ids = []
                    batch_start_time = time.time()

            except Exception as e:
                error_msg = f"Record failed: {str(e)}"
                logging.error(error_msg)
                failed_writer.writerow([record, error_msg])
                failure_count += 1

        if current_batch:
            try:
                if connection.in_transaction:
                    connection.rollback()
                connection.start_transaction()

                for i, (sql, params) in enumerate(current_batch, 1):
                    try:
                        cursor.execute(sql, params)
                    except Exception as e:
                        logging.error(f"Failed to execute statement {i}: {sql} with values {params}")
                        raise

                connection.commit()
                success_count += len(batch_record_ids)
                total_inserted += len(batch_record_ids)
                time_taken = time.time() - batch_start_time
                logging.info(f"✅ Total inserted: {total_inserted} | 🟢 Success: {success_count} | 🔴 Failed: {failure_count} | ⏱️ Time taken: {time_taken:.2f}s")
                update_sequence()
            except Exception as e:
                connection.rollback()
                error_msg = f"Final batch failed: {str(e)}"
                logging.error(error_msg)
                for record_id in batch_record_ids:
                    failed_writer.writerow([f"Record ID {record_id}", error_msg])
                failure_count += len(batch_record_ids)

    cursor.close()
    logging.info(f"🔚 Insert operation complete. 🟢 Success: {success_count}, 🔴 Failure: {failure_count}")

def main():
    config_file = sys.argv[1] if len(sys.argv) > 1 else 'config.json'
    setup_logger()
    logging.info(f"Using config file: {config_file}")
    config = load_config(config_file)
    field_details = load_field_details(config)
    connection = connect_to_db(config)

    base_query = """select
    spec.label as specimen_label,
    spec.identifier as specimen_id,
    spec.collection_protocol_id as cp_id,
    form.de_a_15 as form_nc_details,
    form.de_a_16 as form_nc_reason,
    form.de_a_23 as form_sop,
    form.de_a_37 as form_storage_container,
    form.de_a_39 as form_technitian,
    custom_field.identifier as custom_field_record_id,
    custom_field_form_context_id,
    custom_field.DE_A_12 as custom_field_nc_details,
    non_conf_reason_value,
    sop_value,
    custom_field.de_a_4 as custom_field_storage_tube,
    custom_field.de_a_5 as custom_field_request_id
from
    catissue_specimen spec
    join (
        select
            rec.object_id,
            form.identifier,
            form.de_a_15,
            form.de_a_16,
            form.de_a_23,
            form.de_a_37,
            form.de_a_39
        from
            catissue_form_record_entry rec
            join catissue_form_context ctxt on rec.form_ctxt_id = ctxt.identifier
            join DE_E_11056 form on form.identifier = rec.record_id
        where
            ctxt.container_id = 178
    ) form on form.object_id = spec.identifier
    left join (
        select
            rec.object_id,
            rec.form_ctxt_id as custom_field_form_context_id,
            custom_field.identifier,
            custom_field.DE_A_12,
            custom_field.de_a_4,
            custom_field.de_a_5,
            non_conf_reason.value as non_conf_reason_value,
            sop.value as sop_value
        from
            catissue_form_record_entry rec
            join catissue_form_context ctxt on rec.form_ctxt_id = ctxt.identifier
            join DE_E_11051 custom_field on custom_field.identifier = rec.record_id
            left join DE_E_11055 non_conf_reason on non_conf_reason.record_id = custom_field.identifier
            left join DE_E_11052 sop on sop.record_id = custom_field.identifier
        where
            ctxt.container_id = 176
    ) custom_field on custom_field.object_id = spec.identifier
where
    spec.activity_status != 'Disabled'
    """

    # Replace with actual base query
    for batch in fetch_records_in_batches(connection, base_query):
        to_update, to_insert = divide_records(batch)
        cp_ids = [rec['cp_id'] for rec in to_insert]
        form_ctxt_map = fetch_form_ctxt_ids(connection, cp_ids)
        insert_records(connection, to_insert, form_ctxt_map, field_details)

    connection.close()
    logging.info("Script completed successfully.")

if __name__ == '__main__':
    main()

