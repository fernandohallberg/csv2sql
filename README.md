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
