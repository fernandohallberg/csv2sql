# CSV to MySQL Importer

Automatize a importação de múltiplos arquivos CSV para tabelas MySQL com validação, mapeamento de colunas, tipos de dados, inserção em lotes e muito mais!

## ✅ Recursos

- Suporte a múltiplos arquivos `*.csv`.
- Mapeamento de colunas via JSON.
- Tipos de dados (`dtypes`) via JSON.
- Inserção em lotes (`--chunksize`).
- Dry-run (`--dry-run`) para simulação.
- Logging em arquivo e console.
- Truncate automático no primeiro arquivo.
- Validação opcional via `--force`.
- Verbose mode (`--verbose`).

## ✅ Exemplo de uso

```bash
python3 script.py --csv data/*.csv --tabela minha_tabela --map mapeamento.json --dtypes tipos.json --chunksize 500 --verbose

## ✅ csv2sql.py -h

usage: csv2sql.py [-h] --csv CSV [CSV ...] [--tabela TABELA] [--truncate] [--encoding ENCODING] [--sep SEP] [--conf CONF]
                  [--database DATABASE] [--map MAP] [--skip-header] [--dtypes DTYPES] [--no-header] [--force] [--logfile LOGFILE]
                  [--dry-run] [--chunksize CHUNKSIZE] [--verbose]

Importar múltiplos CSVs para MySQL com validação, mapeamento, dtypes, log, dry-run e inserção em lotes.

options:
  -h, --help            show this help message and exit
  --csv CSV [CSV ...]   Um ou mais arquivos CSV (suporta wildcard).
  --tabela TABELA       Nome da tabela no banco de dados.
  --truncate            Truncate a tabela antes de inserir (apenas no primeiro arquivo).
  --encoding ENCODING   Codificação do CSV.
  --sep SEP             Separador do CSV.
  --conf CONF           Caminho para .env.
  --database DATABASE   Banco de dados a ser usado.
  --map MAP             JSON com mapeamento de colunas.
  --skip-header         Ignorar a primeira linha do CSV.
  --dtypes DTYPES       JSON com dtypes para pandas.read_csv.
  --no-header           CSV sem cabeçalho, usa colunas da tabela.
  --force               Ignora validações de colunas e quantidade.
  --logfile LOGFILE     Arquivo de log (padrão: import_csv.log).
  --dry-run             Simula a importação sem inserir no banco.
  --chunksize CHUNKSIZE
                        Número de linhas por lote na inserção.
  --verbose             Ativa modo detalhado de logging (DEBUG).
