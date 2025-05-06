import sys
import json
import os
import logging
import mysql.connector
import time
import csv
from datetime import datetime

REQUIRED_CONFIG_KEYS = ['host', 'user', 'password', 'database', 'fieldDetailsCsvPath']

def setup_logger():
    log_file = f"logs/script_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
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
                break
            yield records
        offset += batch_size

def fetch_form_ctxt_ids(connection, cp_ids):
    cp_ids = list(set(cp_ids))
    cp_ids_str = ', '.join(map(str, cp_ids))
    if not cp_ids:
        return {}
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

    return to_update, to_insert

def is_empty_value(value):
    if value is None:
        return True
    if isinstance(value, str):
        value = value.strip().lower()
        return not value or value == 'null' or value == 'none'
    return False

def sanitize_records(records):
    for rec in records:
        for k, v in list(rec.items()):
            if is_empty_value(v):
                rec[k] = ''

def insert_records(connection, records, form_ctxt_map, field_details, failed_log_file='failed_inserts.csv'):
    cursor = connection.cursor()
    cursor.execute('SELECT MAX(record_id) FROM catissue_form_record_entry')
    record_id = cursor.fetchone()[0] or 1

    batch_size = 100
    
    with open(failed_log_file, mode='w', newline='', encoding='utf-8') as failed_log:
        failed_writer = csv.writer(failed_log)
        failed_writer.writerow(['Record', 'Error'])

        success_count = 0
        failure_count = 0

        field_mappings = [{
            'legacy_field': row['Legacy Field Name'].strip(),
            'target_field': row['Target Field Name'].strip(),
            'target_table': row['Target Table Name'].strip(),
            'target_column': row['Target Column Name'].strip(),
            'is_multiselect': row['Is Multi-Select'].strip().lower() == 'yes'
        } for row in field_details]

        batch_records = []
        sanitize_records(records)

        for record in records:
            batch_records.append(record)
            if len(batch_records) < batch_size:
                continue

            try:
                for rec in batch_records:
                    record_id += 1
                    cp_id = rec['cp_id']
                    form_ctxt_id = form_ctxt_map.get(str(cp_id))
                    if not form_ctxt_id:
                        raise ValueError(f"No form context ID found for CP ID {cp_id}")

                    cursor.execute('''
                        INSERT INTO catissue_form_record_entry 
                        (FORM_CTXT_ID, OBJECT_ID, RECORD_ID, UPDATED_BY, ACTIVITY_STATUS, FORM_STATUS)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    ''', (
                        form_ctxt_id,
                        rec['specimen_id'],
                        record_id,
                        2,
                        'Active',
                        'COMPLETE'
                    ))

                    for mapping in field_mappings:
                        legacy_field_name = mapping['legacy_field']
                        if legacy_field_name not in rec:
                            continue

                        value = rec[legacy_field_name]
                        if is_empty_value(value):
                            continue

                        if mapping['is_multiselect']:
                            values = [v.strip() for v in str(value).split(',') if v.strip()]
                            for val in values:
                                if val:
                                    cursor.execute(f'''
                                        INSERT INTO {mapping['target_table']} (VALUE, RECORD_ID) 
                                        VALUES (%s, %s)
                                        ON DUPLICATE KEY UPDATE VALUE = VALUES(VALUE);
                                    ''', (val, record_id))
                        else:
                            cursor.execute(f'''
                                INSERT INTO {mapping['target_table']} (IDENTIFIER, {mapping['target_column']})
                                VALUES (%s, %s)
                                ON DUPLICATE KEY UPDATE {mapping['target_column']} = VALUES({mapping['target_column']});
                            ''', (record_id, value))

                cursor.execute('''
                    UPDATE dyextn_id_seq
                    SET LAST_ID = (SELECT MAX(record_id) + 1 FROM catissue_form_record_entry)
                    WHERE TABLE_NAME = 'RECORD_ID_SEQ'
                ''')
                connection.commit()
                success_count += len(batch_records)
                batch_records = []

            except Exception as e:
                connection.rollback()
                for r in batch_records:
                    failed_writer.writerow([r, str(e)])
                failure_count += len(batch_records)
                batch_records = []

        if batch_records:
            try:
                for rec in batch_records:
                    record_id += 1
                    cp_id = rec['cp_id']
                    form_ctxt_id = form_ctxt_map.get(str(cp_id))
                    if not form_ctxt_id:
                        raise ValueError(f"No form context ID found for CP ID {cp_id}")

                    cursor.execute('''
                        INSERT INTO catissue_form_record_entry 
                        (FORM_CTXT_ID, OBJECT_ID, RECORD_ID, UPDATED_BY, ACTIVITY_STATUS, FORM_STATUS)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    ''', (
                        form_ctxt_id,
                        rec['specimen_id'],
                        record_id,
                        2,
                        'Active',
                        'COMPLETE'
                    ))

                    for mapping in field_mappings:
                        legacy_field_name = mapping['legacy_field']
                        if legacy_field_name not in rec:
                            continue

                        value = rec[legacy_field_name]
                        if is_empty_value(value):
                            continue

                        if mapping['is_multiselect']:
                            values = [v.strip() for v in str(value).split(',') if v.strip()]
                            for val in values:
                                if val:
                                    cursor.execute(f'''
                                        INSERT INTO {mapping['target_table']} (VALUE, RECORD_ID) 
                                        VALUES (%s, %s)
                                        ON DUPLICATE KEY UPDATE VALUE = VALUES(VALUE);
                                    ''', (val, record_id))
                        else:
                            cursor.execute(f'''
                                INSERT INTO {mapping['target_table']} (IDENTIFIER, {mapping['target_column']})
                                VALUES (%s, %s)
                                ON DUPLICATE KEY UPDATE {mapping['target_column']} = VALUES({mapping['target_column']});
                            ''', (record_id, value))

                cursor.execute('''
                    UPDATE dyextn_id_seq
                    SET LAST_ID = (SELECT MAX(record_id) + 1 FROM catissue_form_record_entry)
                    WHERE TABLE_NAME = 'RECORD_ID_SEQ'
                ''')
                connection.commit()
                success_count += len(batch_records)

            except Exception as e:
                connection.rollback()
                for r in batch_records:
                    failed_writer.writerow([r, str(e)])
                failure_count += len(batch_records)

    cursor.close()
    return success_count, failure_count

def update_records(connection, records, form_ctxt_map, field_details, failed_log_file='failed_updates.csv'):
    cursor = connection.cursor()
    sanitize_records(records)

    udp_mappings = [{
        'legacy_field': row['Legacy Field Name'].strip(),
        'target_table': row['Target Table Name'].strip(),
        'target_column': row['Target Column Name'].strip(),
        'is_multiselect': row['Is Multi-Select'].strip().lower() == 'yes'
    } for row in field_details]


    header_needed = not os.path.exists(failed_log_file)
    with open(failed_log_file, mode='a', newline='', encoding='utf-8') as failed_log:
        writer = csv.writer(failed_log)
        if header_needed:
            writer.writerow(['Record', 'Error'])

        batch_size = 100
        success_count = 0
        failure_count = 0

        for i in range(0, len(records), batch_size):
            chunk = records[i:i + batch_size]

            for rec in chunk:
                try:
                    record_id = rec['custom_field_record_id']
                    if not record_id:
                        raise ValueError("Missing record_id for update")

                    for m in udp_mappings:
                        legacy_val = rec.get(m['legacy_field'], '')
                        if is_empty_value(legacy_val):
                            continue

                        if m['is_multiselect']:
                            new_vals = [v.strip() for v in str(legacy_val).split(',') if v.strip()]
                            if not new_vals:
                                continue

                            cursor.execute(
                                f"SELECT VALUE FROM {m['target_table']} WHERE RECORD_ID = %s",
                                (record_id,)
                            )
                            existing = {row[0] for row in cursor.fetchall()}

                            for val in new_vals:
                                if val and val not in existing:
                                    cursor.execute(
                                        f"INSERT INTO {m['target_table']}(VALUE, RECORD_ID) VALUES(%s, %s)",
                                        (val, record_id)
                                    )
                        else:
                            clean_val = str(legacy_val).strip()
                            cursor.execute(
                                f"SELECT {m['target_column']} FROM {m['target_table']} WHERE IDENTIFIER = %s",
                                (record_id,)
                            )
                            row = cursor.fetchone()
                            current_val = row[0] if row else None

                            if current_val is None:
                                cursor.execute(
                                    f"UPDATE {m['target_table']} SET {m['target_column']} = %s WHERE IDENTIFIER = %s",
                                    (clean_val, record_id)
                                )
                            elif str(current_val).strip() != clean_val:
                                cursor.execute(
                                    f"UPDATE {m['target_table']} SET {m['target_column']} = %s WHERE IDENTIFIER = %s",
                                    (clean_val, record_id)
                                )

                    success_count += 1

                except Exception as e:
                    connection.rollback()
                    writer.writerow([rec.get('specimen_label', 'UNKNOWN'), str(e)])
                    failure_count += 1

            connection.commit()

    cursor.close()
    return success_count, failure_count

def main():
    start_time = time.time()
    config_file = sys.argv[1] if len(sys.argv) > 1 else 'config.json'
    setup_logger()
    config = load_config(config_file)
    field_details = load_field_details(config)
    connection = connect_to_db(config)

    base_query = """select
    spec.label as specimen_label,
    spec.identifier as specimen_id,
    spec.collection_protocol_id as cp_id,
    form.de_a_15 as "Non-Conformance Detail (specimen)",
    form.de_a_16 as "Non-Conformance Reason (specimen)",
    form.de_a_23 as "Processing SOP Version",
    form.de_a_37 as "Storage Container",
    form.de_a_39 as "Technician",
    custom_field.identifier as custom_field_record_id,
    custom_field_form_context_id,
    custom_field.DE_A_12 as "Non-Conformance Details",
    non_conf_reason_value as "Non-Conformances",
    sop_value as "SOP",
    custom_field.de_a_4 as "Storage Tube",
    custom_field.de_a_5 as "Request ID"
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

    total_inserted = 0
    total_updated = 0
    total_failed = 0
    processed_count = 0

    for batch in fetch_records_in_batches(connection, base_query):
        to_update, to_insert = divide_records(batch)
        cp_ids = [rec['cp_id'] for rec in to_insert]
        form_ctxt_map = fetch_form_ctxt_ids(connection, cp_ids)

        inserted = 0
        updated = 0
        failed = 0

        if to_insert:
            inserted, insert_failed = insert_records(connection, to_insert, form_ctxt_map, field_details)
            failed += insert_failed

        if to_update:
            updated, update_failed = update_records(connection, to_update, form_ctxt_map, field_details)
            failed += update_failed

        total_inserted += inserted
        total_updated += updated
        total_failed += failed
        processed_count += len(batch)

        logging.info(f"Total: {processed_count} | inserted: {total_inserted} | updated: {total_updated} | failed: {total_failed}")

    connection.close()
    elapsed_time = time.time() - start_time
    logging.info(f"Processing completed in {elapsed_time:.2f} seconds")
    logging.info(f"Final totals - Total: {processed_count} | inserted: {total_inserted} | updated: {total_updated} | failed: {total_failed}")

if __name__ == '__main__':
    main()
