import subprocess
import sys
import logging
import argparse
import pandas as pd
from pathlib import Path
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_script(script_path, args=None):
    command = [sys.executable, str(script_path)]
    if args:
        command.extend(args)
    
    logging.info(f"Запуск скрипта: {script_path}")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    stdout, stderr = process.communicate()
    
    if process.returncode != 0:
        logging.error(f"Ошибка при выполнении {script_path}:")
        logging.error(stderr)
        sys.exit(1)
    else:
        logging.info(f"Скрипт {script_path} успешно выполнен")
        logging.debug(stdout)

def main():
    parser = argparse.ArgumentParser(description="Система обработки данных о ферментах")
    parser.add_argument('--enzyme', type=str, help="Название фермента для поиска")
    parser.add_argument('--file', type=Path, help="Путь к файлу с названиями ферментов (колонка 'Protein')")
    parser.add_argument('--names_ec_path', type=Path, default=Path('names_ec.py'), help="Путь к names_ec.py")
    parser.add_argument('--ec_entries_path', type=Path, default=Path('ec_entries.py'), help="Путь к ec_entries.py")
    parser.add_argument('--entries_sequence_path', type=Path, default=Path('ent_seq_v2.py'), help="Путь к entries_sequence.py")
    parser.add_argument('--smile_spider_path', type=Path, default=Path('C:/Users/vasae/parsing/smiles/smiles/spiders/smile_spider.py'), help="Путь к smile_spider.py")
    parser.add_argument('--use-diamond', action='store_true', help="Включить функционал DIAMOND для анализа")
    
    # Аргументы для DIAMOND
    parser.add_argument('--query', type=str, help="Путь к входному файлу FASTA для DIAMOND")
    parser.add_argument('--db', type=str, help="Путь к базе данных DIAMOND")
    parser.add_argument('--out', type=str, help="Путь к выходному файлу результатов DIAMOND")
    parser.add_argument('--threads', type=int, default=4, help="Количество потоков для DIAMOND")
    parser.add_argument('--sensitive', action='store_true', help="Использовать чувствительный режим DIAMOND")
    
    args = parser.parse_args()

    if args.use_diamond:
        # Запуск только DIAMOND анализа
        diamond_args = [
            '--use-diamond',
            '--query', args.query,
            '--db', args.db,
            '--out', args.out,
            '--threads', str(args.threads)
        ]
        if args.sensitive:
            diamond_args.extend(['--diamond-mode', 'sensitive'])
        
        run_script(args.entries_sequence_path, diamond_args)
    else:
        # Стандартный поток выполнения для анализа ферментов
        enzymes = []
        
        if args.file:
            # Чтение значений из файла
            try:
                df = pd.read_excel(args.file)
                enzymes = df['Protein'].tolist()
                logging.info(f"Файл '{args.file}' успешно прочитан. Найдено {len(enzymes)} записей.")
            except Exception as e:
                logging.error(f"Ошибка чтения файла '{args.file}': {e}")
                sys.exit(1)
        elif args.enzyme:
            enzymes.append(args.enzyme)
        else:
            logging.error("Необходимо указать либо название фермента, либо путь к файлу.")
            sys.exit(1)

        # Запуск скриптов по очереди для каждого фермента
        for enzyme in enzymes:
            run_script(args.names_ec_path, [enzyme])
            run_script(args.ec_entries_path)
            run_script(args.entries_sequence_path)
            run_script(args.smile_spider_path)

        logging.info("Все скрипты успешно выполнены. Результаты сохранены в файл Final_results.xlsx")

if __name__ == "__main__":
    main()