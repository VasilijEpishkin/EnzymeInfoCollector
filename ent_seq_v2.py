import sys
import os
import logging
import redis
import json
import argparse
import requests
from multiprocessing import Pool, Manager
import pandas as pd
from io import StringIO
from time import sleep
from tqdm import tqdm
import subprocess
import shutil

os.environ["PATH"] += os.pathsep + "/usr/bin"
# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Подключение к Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def string4mapping(columns=None):
    columns_dict = {
        'Entry': 'accession',
        'Entry Name': 'id',
        'Organism': 'organism_name',
        'Organism ID': 'organism_id',
        'Gene Names': 'gene_names',
        'Protein names': 'protein_name',
        'EC number': 'ec',
        'Sequence': 'sequence',
        'Length': 'length',
        'RefSeq': 'xref_refseq',
        'Status': 'reviewed',
    }
    return ','.join([columns_dict[column] for column in columns]) if columns else None

def uniprot_request(ids, columns=None, output_format='tsv'):
    fields = f'&fields={string4mapping(columns=columns)}' if columns else ''
    url = f"https://rest.uniprot.org/uniprotkb/accessions?accessions={','.join(ids)}{fields}&format={output_format}"
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def get_uniprot_information(ids, columns=None, step=1000, sleep_time=3, max_tries=3):
    result = pd.DataFrame()
    session = requests.Session()  # Используем сессию для ускорения запросов

    for i in tqdm(range(0, len(ids), step), desc='Fetching UniProt data'):
        tries = 0
        done = False
        j = min(i + step, len(ids))
        while not done and tries < max_tries:
            try:
                data = uniprot_request(ids[i:j], columns=columns)
                if data:
                    uniprot_info = pd.read_csv(StringIO(data), sep='\t')
                    result = pd.concat([result, uniprot_info])
                done = True
            except requests.RequestException as e:
                logging.error(f'ID mapping failed. Attempt {tries+1} of {max_tries}. Error: {e}')
                tries += 1
                sleep(sleep_time)
    return result

def fetch_protein_data_by_ac(entry):
    try:
        logging.info(f"Fetching data for UniProtKB AC '{entry}'")
        result = get_uniprot_information(
            [entry],
            columns=['Entry', 'Entry Name', 'Protein names', 'Gene Names', 'EC number', 'Organism', 
                     'Organism ID', 'Sequence', 'Length', 'RefSeq', 'Status']
        )
        if not result.empty:
            return result.to_dict('records')
        else:
            logging.info(f"No data found for UniProtKB AC '{entry}'")
            return []
    except Exception as e:
        logging.error(f"Error fetching data for UniProtKB AC '{entry}': {e}")
        return []

def fetch_and_process_data(entry):
    try:
        data = fetch_protein_data_by_ac(entry)
        if data:
            logging.info(f"Found {len(data)} results for Entry '{entry}'")
            return data
        else:
            logging.info(f"No data found for Entry '{entry}'")
            return []
    except Exception as e:
        logging.error(f"Error fetching data for Entry '{entry}': {e}")
        return []

def parallel_fetch(entries, func, num_of_processes=8):
    with Manager() as manager:
        with Pool(processes=num_of_processes) as pool:
            results = pool.map(func, entries)
    return [item for sublist in results for item in sublist]

def make_diamond_database(fasta_file, database_path):
    command = [
        'diamond', 'makedb',
        '--in', fasta_file,
        '--db', database_path
    ]
    process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if process.returncode != 0:
        logging.error(f"Error creating DIAMOND database: {process.stderr}")
        sys.exit(1)
    else:
        logging.info("DIAMOND database created successfully.")

def run_diamond(query, database, output, threads, mode):
    # Проверяем, доступен ли DIAMOND в системе
    diamond_path = shutil.which('diamond')
    if not diamond_path:
        logging.error("DIAMOND не найден в системе. Убедитесь, что DIAMOND установлен и доступен в PATH.")
        sys.exit(1)

    command = [
        diamond_path,
        'blastp',
        '--query', query,
        '--db', database,
        '--out', output,
        '--threads', str(threads),
        '--outfmt', '6',
        '--sensitive' if mode == 'sensitive' else '--fast'
    ]
    
    logging.info(f"Запуск команды DIAMOND: {' '.join(command)}")
    
    try:
        process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        logging.info("DIAMOND search completed successfully.")
        logging.debug(f"DIAMOND output: {process.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running DIAMOND search: {e.stderr}")
        sys.exit(1)
    except FileNotFoundError:
        logging.error(f"DIAMOND executable not found at {diamond_path}. Please make sure DIAMOND is installed correctly.")
        sys.exit(1)

def run_diamond_processes(args):
    # Функция для работы только с DIAMOND
    fasta_file = "input_sequences.fasta"
    diamond_db = f"{args.database}.dmnd"
    aligned_output = "diamond_results.m8"
    
    # Создание базы данных DIAMOND
    if not os.path.isfile(diamond_db):
        make_diamond_database(fasta_file, diamond_db)
    
    # Запуск DIAMOND поиска
    run_diamond(
        query=fasta_file,
        database=diamond_db,
        output=aligned_output,
        threads=args.threads,
        mode=args.diamond_mode
    )

def save_to_fasta(data, output_file='output_sequences.fasta'):
    with open(output_file, 'w') as fasta_file:
        for entry in data:
            entry_id = entry.get("Entry", "unknown")
            protein_name = entry.get("Protein names", "unknown_protein")
            sequence = entry.get("Sequence", "")

            # Пропускаем, если последовательность пуста
            if not sequence:
                continue

            # Записываем в формате FASTA
            fasta_file.write(f">{entry_id} {protein_name}\n")
            # Разбиваем последовательность на строки по 60 символов для читаемости
            for i in range(0, len(sequence), 60):
                fasta_file.write(sequence[i:i + 60] + '\n')

    logging.info(f"FASTA файл успешно создан: {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Скрипт для анализа данных с использованием DIAMOND")
    parser.add_argument('--use-diamond', action='store_true', help="Включить функционал DIAMOND для анализа")
    parser.add_argument('--query', help="Путь к файлу запроса для DIAMOND")
    parser.add_argument('--db', help="Путь к базе данных DIAMOND")
    parser.add_argument('--out', help="Путь к выходному файлу DIAMOND")
    parser.add_argument('--threads', type=int, default=os.cpu_count(), help="Количество потоков для DIAMOND")
    parser.add_argument('--diamond-mode', default='fast', choices=['fast', 'sensitive'], help="Режим работы DIAMOND")
    args = parser.parse_args()

    if args.use_diamond:
        if not all([args.query, args.db, args.out]):
            logging.error("Для использования DIAMOND необходимо указать --query, --db и --out.")
            sys.exit(1)
        
        run_diamond(
            query=args.query,
            database=args.db,
            output=args.out,
            threads=args.threads,
            mode=args.diamond_mode
        )
    else:
        # Получаем UniProt записи из Redis
        uniprot_entries_json = redis_client.get('uniprot_entries')
        if not uniprot_entries_json:
            logging.error("No UniProt entries found in Redis")
            return

        uniprot_entries = json.loads(uniprot_entries_json)
        entries = [entry['Entries'] for entry in uniprot_entries if 'Entries' in entry]
        entries = [item for sublist in entries for item in sublist.split('\n') if item]

        results = parallel_fetch(entries, fetch_and_process_data, num_of_processes=os.cpu_count())
        
        # Сохраняем результаты в Redis
        redis_client.set('ent_seq_results', json.dumps(results))
    
        save_to_fasta(results)
    
    logging.info("Script execution completed")

if __name__ == "__main__":
    main()