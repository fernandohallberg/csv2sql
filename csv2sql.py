#!/usr/bin/env python3
import os
import sys
import json
import pandas as pd
import sqlalchemy as sa
import argparse
import logging
from sqlalchemy import text
from dotenv import load_dotenv

def configurar_logger(logfile, verbose=False):
    logger = logging.getLogger()
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

def carregar_dotenv(conf_path=None):
    env_path = conf_path or os.path.join(os.getcwd(), '.env')
    load_dotenv(dotenv_path=env_path)
    logging.info(f"Carregado .env de: {env_path}")

def tabela_existe(conn, table_name):
    result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}';")).fetchone()
    return result is not None

def banco_existe(engine, db_name):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{db_name}';"))
            return result.fetchone() is not None
    except Exception as e:
        logging.error(f"Erro ao verificar existência do banco de dados '{db_name}': {e}")
        return False

def carregar_json(path, descricao):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logging.info(f"{descricao} carregado com sucesso: {data}")
        return data
    except Exception as e:
        logging.error(f"Erro ao carregar {descricao} {path}: {e}")
        sys.exit(1)

def colunas_tabela(engine, db_name, table_name):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{db_name}'
                  AND TABLE_NAME = '{table_name}';
            """))
            colunas = [row[0] for row in result.fetchall()]
            logging.info(f"Colunas da tabela '{table_name}': {colunas}")
            return colunas
    except Exception as e:
        logging.error(f"Erro ao obter colunas da tabela {table_name}: {e}")
        sys.exit(1)

def validar_colunas(colunas_mapeadas, colunas_tabela, force=False):
    if force:
        logging.warning("Validação de colunas ignorada devido ao modo --force.")
        return
    colunas_faltantes = set(colunas_mapeadas) - set(colunas_tabela)
    if colunas_faltantes:
        logging.error(f"Colunas do mapeamento não existem na tabela: {colunas_faltantes}")
        sys.exit(1)
    else:
        logging.info("Validação: todas as colunas do mapeamento existem na tabela.")

def validar_numero_colunas(df, colunas_banco, force=False):
    if force:
        logging.warning("Validação do número de colunas ignorada devido ao modo --force.")
        return
    if df.shape[1] != len(colunas_banco):
        logging.error(f"Número de colunas no CSV ({df.shape[1]}) difere da tabela ({len(colunas_banco)}).")
        sys.exit(1)
    else:
        logging.info("Validação: número de colunas compatível com a tabela.")

def parse_validacoes(validate_fields_str):
    validacoes = {}
    if not validate_fields_str:
        return validacoes
    for item in validate_fields_str.split(';'):
        campo, regra = item.split(',')
        validacoes[campo.strip()] = regra.strip()
    return validacoes

def aplicar_validacoes(df, validacoes):
    for campo, regra in validacoes.items():
        if campo not in df.columns:
            logging.warning(f"Campo '{campo}' não encontrado no DataFrame para validação.")
            continue
        if regra == 'notnull':
            antes = len(df)
            df = df[df[campo].notnull() & (df[campo].astype(str).str.strip() != '')]
            depois = len(df)
            logging.info(f"Validação '{campo} notnull': {antes} → {depois} linhas.")
        else:
            logging.warning(f"Regra de validação '{regra}' não implementada para o campo '{campo}'.")
    return df

_FLOAT_DTYPE_NAMES = {'float', 'float16', 'float32', 'float64', 'float128',
                      'Float32', 'Float64'}

def _separar_dtypes_float(dtypes, decimal):
    """Quando decimal != '.', retira colunas float do dtype para leitura como object,
    pois o pandas não aplica o separador decimal em tipos nullable (Float64) ou numpy float.
    Retorna (read_dtypes, {col: dtype_original}) para conversão posterior."""
    if not dtypes or decimal == '.':
        return dtypes, {}
    read_dtypes = dict(dtypes)
    float_cols = {}
    for col, dtype in dtypes.items():
        if str(dtype) in _FLOAT_DTYPE_NAMES:
            float_cols[col] = dtype
            read_dtypes[col] = 'object'
    return read_dtypes, float_cols

def _converter_float_cols(df, float_cols, decimal):
    """Converte colunas lidas como object para o dtype float original,
    trocando o separador decimal antes da conversão."""
    for col, dtype in float_cols.items():
        if col not in df.columns:
            continue
        try:
            df[col] = (df[col]
                       .astype(str)
                       .str.strip()
                       .str.replace(decimal, '.', regex=False)
                       .pipe(pd.to_numeric, errors='coerce')
                       .astype(dtype))
            logging.info(f"Coluna '{col}' convertida para {dtype} com decimal='{decimal}'.")
        except Exception as e:
            logging.warning(f"Erro ao converter coluna '{col}' para {dtype}: {e}")
    return df

def import_csv_to_mysql(csv_file, table_name, engine, db_name, mapeamento=None, truncate=False, encoding='utf-8',
                        sep=';', skip_header=False, dtypes=None, no_header=False, force=False, dry_run=False,
                        chunksize=None, validacoes=None, decimal='.'):
    skip = 1 if skip_header else 0
    colunas_banco = colunas_tabela(engine, db_name, table_name)
    read_dtypes, float_cols = _separar_dtypes_float(dtypes, decimal)
    try:
        if no_header:
            logging.info(f"Modo sem cabeçalho ativado. Usando colunas da tabela: {colunas_banco}")
            df = pd.read_csv(csv_file, encoding=encoding, sep=sep, header=None, names=colunas_banco,
                             dtype=read_dtypes, skiprows=skip, decimal=decimal)
        else:
            df = pd.read_csv(csv_file, encoding=encoding, sep=sep, skiprows=skip, dtype=read_dtypes, decimal=decimal)
        logging.info(f"CSV '{csv_file}' carregado com sucesso. Linhas: {len(df)}.")
    except UnicodeDecodeError as e:
        logging.warning(f"Erro UTF-8: {e}, tentando ISO-8859-1...")
        try:
            if no_header:
                df = pd.read_csv(csv_file, encoding='ISO-8859-1', sep=sep, header=None, names=colunas_banco,
                                 dtype=read_dtypes, skiprows=skip, decimal=decimal)
            else:
                df = pd.read_csv(csv_file, encoding='ISO-8859-1', sep=sep, skiprows=skip, dtype=read_dtypes, decimal=decimal)
            logging.info(f"CSV '{csv_file}' carregado com sucesso usando ISO-8859-1.")
        except Exception as e:
            logging.error(f"Erro ao ler CSV com ISO-8859-1: {e}")
            return
    except pd.errors.ParserError as e:
        logging.error(f"Erro de parsing ao ler CSV: {e}")
        return

    if float_cols:
        df = _converter_float_cols(df, float_cols, decimal)

    validar_numero_colunas(df, colunas_banco, force=force)

    if mapeamento:
        df = df.rename(columns=mapeamento)
        colunas_mapeadas = list(mapeamento.values())
        validar_colunas(colunas_mapeadas, colunas_banco, force=force)
        df = df[colunas_mapeadas]
        logging.info(f"Colunas renomeadas e filtradas: {colunas_mapeadas}")

    if validacoes:
        df = aplicar_validacoes(df, validacoes)

    if truncate:
        try:
            with engine.connect() as conn:
                if tabela_existe(conn, table_name):
                    conn.execute(text(f"TRUNCATE TABLE {table_name};"))
                    logging.info(f"Tabela {table_name} truncada com sucesso.")
                else:
                    logging.error(f"Tabela {table_name} não existe.")
        except Exception as e:
            logging.error(f"Erro ao truncar a tabela {table_name}: {e}")
            return

    if dry_run:
        logging.info(f"Dry-run ativado: dados não serão inseridos no banco.")
        return

    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=chunksize)
        logging.info(f"Dados inseridos com sucesso na tabela '{table_name}' (linhas: {len(df)}).")
    except Exception as e:
        logging.error(f"Erro ao inserir os dados na tabela {table_name}: {e}")

def main():
    parser = argparse.ArgumentParser(
        description="Importar múltiplos CSVs para MySQL com validação, mapeamento, dtypes, log, dry-run, inserção em lotes e validação de campos.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--csv', required=True, nargs='+', help="Um ou mais arquivos CSV (suporta wildcard).")
    parser.add_argument('--tabela', required=False, help="Nome da tabela no banco de dados.")
    parser.add_argument('--truncate', action='store_true', help="Truncate a tabela antes de inserir (apenas no primeiro arquivo).")
    parser.add_argument('--encoding', default='utf-8', help="Codificação do CSV.")
    parser.add_argument('--sep', default=';', help="Separador do CSV.")
    parser.add_argument('--conf', required=False, help="Caminho para .env.")
    parser.add_argument('--database', required=False, help="Banco de dados a ser usado.")
    parser.add_argument('--map', required=False, help="JSON com mapeamento de colunas.")
    parser.add_argument('--skip-header', action='store_true', help="Ignorar a primeira linha do CSV.")
    parser.add_argument('--dtypes', required=False, help="JSON com dtypes para pandas.read_csv.")
    parser.add_argument('--no-header', action='store_true', help="CSV sem cabeçalho, usa colunas da tabela.")
    parser.add_argument('--force', action='store_true', help="Ignora validações de colunas e quantidade.")
    parser.add_argument('--logfile', default='import_csv.log', help="Arquivo de log (padrão: import_csv.log).")
    parser.add_argument('--dry-run', action='store_true', help="Simula a importação sem inserir no banco.")
    parser.add_argument('--chunksize', type=int, default=None, help="Número de linhas por lote na inserção.")
    parser.add_argument('--verbose', action='store_true', help="Ativa modo detalhado de logging (DEBUG).")
    parser.add_argument('--validate-fields', required=False, help="Validações de campos no formato: 'campo,regra;campo,regra'. Ex: 'razaosocial,notnull'.")
    parser.add_argument('--read-dtype', required=False, help="JSON com dtypes para leitura do CSV (pandas.read_csv).")
    parser.add_argument('--write-dtype', required=False, help="JSON com dtypes para escrita no MySQL (SQLAlchemy types).")
    parser.add_argument('--decimal', default='.', help="Separador decimal do CSV (padrão: '.'). Use ',' para floats no formato brasileiro.")
    args = parser.parse_args()

    logger = configurar_logger(args.logfile, verbose=args.verbose)

    if args.chunksize is not None and args.chunksize <= 0:
        logging.error("O valor de --chunksize deve ser um inteiro maior que 0.")
        sys.exit(1)

    validacoes = parse_validacoes(args.validate_fields)
    carregar_dotenv(conf_path=args.conf)

    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = args.database or os.getenv('DB_NAME')
    logging.info(f"Usando banco de dados: {DB_NAME}")

    engine_test = sa.create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/information_schema')
    if not banco_existe(engine_test, DB_NAME):
        logging.error(f"Banco de dados '{DB_NAME}' não existe.")
        sys.exit(1)

    sqluri = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = sa.create_engine(sqluri)

    table_name = args.tabela or os.path.splitext(os.path.basename(args.csv[0]))[0]
    logging.info(f"Tabela destino: {table_name}")

    mapeamento = carregar_json(args.map, "Mapeamento") if args.map else None
    dtypes = carregar_json(args.dtypes, "Dtypes") if args.dtypes else None

    for idx, csv_file in enumerate(args.csv):
        should_truncate = args.truncate and idx == 0
        logging.info(f"\n🚀 Processando arquivo {idx + 1}/{len(args.csv)}: {csv_file}")
        import_csv_to_mysql(
            csv_file,
            table_name,
            engine,
            db_name=DB_NAME,
            mapeamento=mapeamento,
            truncate=should_truncate,
            encoding=args.encoding,
            sep=args.sep,
            skip_header=args.skip_header,
            dtypes=dtypes,
            no_header=args.no_header,
            force=args.force,
            dry_run=args.dry_run,
            chunksize=args.chunksize,
            validacoes=validacoes,
            decimal=args.decimal,
        )

if __name__ == "__main__":
    main()
