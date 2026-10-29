# EnzymeInfoCollector

CLI-инструмент для сбора сведений об энзимах по названию или номеру EC. Объединяет номенклатуру ExPASy ENZYME, аннотации и последовательности белков UniProtKB, реакции и SMILES из Rhea. Сохраняет результаты в JSON, CSV или FASTA с указанием источника и времени получения данных.

## Установка

Требуется Python 3.11 или новее. Для загрузки свежих данных нужен доступ к ExPASy, UniProt и Rhea. Дополнительных зависимостей нет.

~~~bash
python -m venv .venv
source .venv/bin/activate
python -m pip install .
~~~

## Основные команды

~~~bash
# По названию или одному/нескольким номерам EC
enzymeinfocollector --enzyme "Alcohol dehydrogenase" -o enzyme.json
enzymeinfocollector --ec 1.1.1.1 --ec 3.5.1.n3 -o enzymes.csv

# Фильтрация записей UniProt: проверенные записи, таксон и его потомки
enzymeinfocollector --ec 1.1.1.1 --reviewed-only --taxon-id 4932 -o enzymes.json

# Экспорт последовательностей
enzymeinfocollector --ec 1.1.1.1 -o proteins.fasta

# Входной файл CSV, TSV или TXT
enzymeinfocollector --file queries.csv -o enzymes.json
~~~

В CSV или TSV нужен столбец Protein, Enzyme либо EC number. Текстовый файл содержит по одному названию или полному номеру EC на строку; можно добавлять префикс EC перед номером. Тип результата определяется расширением файла или параметром --format. Параметр --no-sequences загружает аннотации без последовательностей; его нельзя использовать для FASTA.

### Подготовка повторяемого запроса

Команда --prepare-manifest проверяет входные значения, удаляет повторы и сохраняет запросы с фильтрами в JSON **без обращения к базам данных** и без создания файла результата. Позже тот же запрос можно выполнить через --replay-manifest:

~~~bash
enzymeinfocollector --file queries.csv --reviewed-only --taxon-id 4932 \
  --save-manifest run.json --prepare-manifest
enzymeinfocollector --replay-manifest run.json -o results.json
~~~

Манифест хранит значения запросов открытым текстом. Он фиксирует параметры запроса, но не снимки баз: результаты повторного запуска могут измениться после обновления источников. Параметр --save-manifest без --prepare-manifest сохраняет манифест и сразу запускает сбор.

## Результаты и ограничения

- JSON сохраняет полные записи об энзимах, белках, реакциях и происхождении ответов. Для больших результатов --stream-json уменьшает расход памяти за счёт временных файлов и дополнительного времени. CSV и FASTA обрабатываются порциями.
- Перенесённые номера EC разрешаются по цепочкам и ветвлениям ENZYME. Исходная запись и пути переноса сохраняются в resolution; количество путей ограничено 10 000 на запрошенный EC, превышение явно помечается как partial/truncated.
- В CSV поле ec_reactions связывает реакцию с номером EC, а не с каждым конкретным белком. SMILES считываются из опубликованных таблиц Rhea; для ненаправленной реакции доступные направленные формы хранятся в reaction_smiles_directions.
- Если ExPASy не находит название или номер EC, результат помечается как not_found либо not_found_in_expasy. Для корректного EC UniProt и Rhea всё равно запрашиваются отдельно. Ответ Rhea, достигший лимита 10 000 строк, отклоняется как потенциально неполный.
- Выходной файл заменяется только после успешного сбора. Сбой источника не подменяется пустым ответом.
- Неполные или некорректные ответы не трактуются как отсутствие записей: пустой каталог ExPASy, отсутствующий запрошенный столбец последовательностей UniProt, записи без accession, неверные идентификаторы Rhea и противоречащие друг другу SMILES вызывают ошибку. Корректный пустой результат поиска UniProt/Rhea при этом допустим.
- SMILES передаются из опубликованных данных Rhea как есть: инструмент проверяет наличие записи и согласованность повторяющихся идентификаторов, **но не подтверждает химическую валидность или корректность реакции через RDKit**. **В существующем CLI** реакции и белки группируются по EC-номеру; это не доказательство катализа для конкретной последовательности. Для прямых аннотаций используйте отдельный Python API ниже.

HTTP-ответы кэшируются в ~/.cache/enzymeinfocollector или $XDG_CACHE_HOME/enzymeinfocollector на срок от одного до семи дней. При временном сбое может использоваться ответ возрастом до 30 дней с пометкой stale. Параметр --refresh запрашивает обновление. Для Rhea рекомендуется указать --contact-email или переменную ENZYMEINFOCOLLECTOR_EMAIL. Состояние кэша можно проверить и очистить:

~~~bash
enzymeinfocollector cache info
enzymeinfocollector cache prune
enzymeinfocollector cache prune --older-than-days 60 --apply
~~~

Очистка без --apply только показывает план; минимальный срок хранения — 30 дней. Файлы блокировок .lock остаются в кэше. При параллельной работе через Python API используйте отдельный экземпляр EnzymeCollector для каждого сбора.

## Экспериментальный Python API: прямые связи UniProt ↔ Rhea

Новый модуль `enzymeinfocollector.evidence_sources` извлекает **индивидуальные**
каталитические аннотации UniProtKB с Rhea ID, ECO-кодами и доступными ссылками
на публикации. Отдельно сохраняются Rhea master ID и направленные
физиологические реакции. Связи **не создаются** из совпадения EC-номеров.

~~~python
from enzymeinfocollector.evidence_sources import DirectEvidenceCollector
from enzymeinfocollector.sources import HttpCache, default_cache_dir

collector = DirectEvidenceCollector(HttpCache(default_cache_dir()))
dataset = collector.collect(["P00330", "P00558"])
for protein in dataset.proteins:
    print(protein.accession, len(protein.assertions))
~~~

Это **дополнительный API**, не новая опция старой CLI-команды. Он сохраняет
релиз UniProt, версии записей, время загрузки и SHA-256 таблицы направлений
Rhea. При устаревших или противоречивых ответах выдаёт ошибку. Отсутствие
аннотации означает «неизвестно», а не «белок не катализирует реакцию».

Для воспроизводимого JSON-экспорта и отдельной проверки на официальном
сопоставлении Rhea Swiss-Prot предусмотрены экспериментальные команды:

~~~bash
python -m enzymeinfocollector.evidence_cli collect --accession P00330 -o evidence.json
python -m enzymeinfocollector.evidence_cli validate --accession P00330 --accession P00331 --refresh
~~~

`validate` сравнивает **основные и направленные идентификаторы Rhea**,
но не доказывает экспериментальную полноту аннотаций. Для TrEMBL эта
проверка не используется: таблица `rhea2uniprot_sprot.tsv` охватывает
только Swiss-Prot. Экспорт и проверки не меняют прежний CLI.

[Описание API, ограничений и проверки на пяти белках](docs/architecture/direct_evidence.md).
Базовые научные контракты: [evidence model](docs/architecture/evidence_model.md).

## Источники и разработка

- [ExPASy ENZYME](https://enzyme.expasy.org/) — номенклатура и номера EC.
- [UniProt REST API](https://rest.uniprot.org/) — белки и последовательности.
- [Rhea REST API](https://www.rhea-db.org/help/rest-api) и [таблицы TSV](https://www.rhea-db.org/help/download) — реакции и направленные SMILES.

При распространении результатов учитывайте лицензии, требования к цитированию и атрибуции каждого источника. Таблица SMILES Rhea имеет экспериментальный статус.

Локальные тесты не требуют доступа к внешним API и используют искусственные ответы источников. Они проверяют успешные сценарии и отказ при повреждённых данных. В том числе `tests/test_data_validation.py` проверяет целостность форматов ExPASy/UniProt/Rhea и обработку конфликтующих записей.

~~~bash
python -m unittest discover -s tests -v
# Дополнительная нагрузочная проверка потокового JSON (Linux)
python benchmarks/stream_json.py
~~~

Эти проверки **не заменяют** запуск на актуальных данных ExPASy/UniProt/Rhea и предметную оценку качества биохимической аннотации. Программа распространяется по [лицензии MIT](LICENSE).
