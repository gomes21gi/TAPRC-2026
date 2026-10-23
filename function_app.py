import logging
import os
import urllib.parse
import urllib.request

import azure.functions as func
import pyodbc

import json
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient, ContentSettings

app = func.FunctionApp()

def salvar_na_bronze(entidade, colunas, rows):
    agora = datetime.now(timezone.utc)
    caminho = (
        f"itsm/{entidade}/ano={agora:%Y}/mes={agora:%m}/dia={agora:%d}/"
        f"{entidade}_{agora:%Y%m%dT%H%M%SZ}.json"
    )
    dados = [dict(zip(colunas, row)) for row in rows]
    conteudo = json.dumps(dados, default=str, ensure_ascii=False)

    service = BlobServiceClient.from_connection_string(os.getenv("STORAGE_CONNECTION_STRING"))
    blob = service.get_blob_client(container="bronze", blob=caminho)
    blob.upload_blob(
        conteudo.encode("utf-8"),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    logging.info(f"{entidade}: {len(dados)} registros gravados em bronze/{caminho}")

# chamado
@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado(myTimer: func.TimerRequest) -> None:
    # importar variáveis de ambiente
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    # como criar uma connection string usando pyodbc
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        # Criar a conexao com o banco
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # fazer um select * na tabela
            cursor.execute("SELECT * FROM itsm.chamado")
            rows = cursor.fetchall()

            # imprimir os dados da tabela usando logging.info()
            if not rows:
                logging.info("A consulta não retornou nenhum dado.")
            else:
                colunas = [c[0] for c in cursor.description]
                salvar_na_bronze("chamado", colunas, rows)

    except pyodbc.Error as e:
        logging.error(f"Erro ao conectar ou consultar o banco de dados: {e}")

    print(host_sql)
    logging.info(user_sql)
    logging.info(database_sql)
    logging.info(host_sql)
    logging.info(password_sql)

    # Criar a conexão com o banco de dados
    # Fazer um select * na tabela
    # Imprimir os dados da tabela usando logging.info


# Chamado_sla
@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_sla(myTimer: func.TimerRequest) -> None:
    # importar variáveis de ambiente
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    # como criar uma connection string usando pyodbc
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        # Criar a conexao com o banco
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # fazer um select * na tabela
            cursor.execute("SELECT * FROM itsm.chamado_sla")
            rows = cursor.fetchall()

            # imprimir os dados da tabela usando logging.info()
                        if not rows:
                logging.info("A consulta não retornou nenhum dado.")
            else:
                colunas = [c[0] for c in cursor.description]
                salvar_na_bronze("chamado_sla", colunas, rows)

    except pyodbc.Error as e:
        logging.error(f"Erro ao conectar ou consultar o banco de dados: {e}")

    print(host_sql)
    logging.info(user_sql)
    logging.info(database_sql)
    logging.info(host_sql)
    logging.info(password_sql)


# Chamado_status_historico
@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_chamado_status_historico(myTimer: func.TimerRequest) -> None:
    # importar variáveis de ambiente
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    # como criar uma connection string usando pyodbc
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        # Criar a conexao com o banco
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # fazer um select * na tabela
            cursor.execute("SELECT * FROM itsm.chamado_status_historico")
            rows = cursor.fetchall()

            # imprimir os dados da tabela usando logging.info()
                        if not rows:
                logging.info("A consulta não retornou nenhum dado.")
            else:
                colunas = [c[0] for c in cursor.description]
                salvar_na_bronze("chamado_status_historico", colunas, rows)

    except pyodbc.Error as e:
        logging.error(f"Erro ao conectar ou consultar o banco de dados: {e}")

    print(host_sql)
    logging.info(user_sql)
    logging.info(database_sql)
    logging.info(host_sql)
    logging.info(password_sql)


# cliente_organizacao
@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_cliente_organizacao(myTimer: func.TimerRequest) -> None:
    # importar variáveis de ambiente
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    # como criar uma connection string usando pyodbc
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        # Criar a conexao com o banco
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # fazer um select * na tabela
            cursor.execute("SELECT * FROM itsm.cliente_organizacao")
            rows = cursor.fetchall()

            # imprimir os dados da tabela usando logging.info()
            if not rows:
                logging.info("A consulta não retornou nenhum dado.")
            else:
                colunas = [c[0] for c in cursor.description]
                salvar_na_bronze("cliente_organizacao", colunas, rows)

    except pyodbc.Error as e:
        logging.error(f"Erro ao conectar ou consultar o banco de dados: {e}")

    print(host_sql)
    logging.info(user_sql)
    logging.info(database_sql)
    logging.info(host_sql)
    logging.info(password_sql)


# analista
@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_analista(myTimer: func.TimerRequest) -> None:
    # importar variáveis de ambiente
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    # como criar uma connection string usando pyodbc
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        # Criar a conexao com o banco
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # fazer um select * na tabela
            cursor.execute("SELECT * FROM itsm.analista")
            rows = cursor.fetchall()

            # imprimir os dados da tabela usando logging.info()
            if not rows:
                logging.info("A consulta não retornou nenhum dado.")
            else:
                colunas = [c[0] for c in cursor.description]
                salvar_na_bronze("analista", colunas, rows)

    except pyodbc.Error as e:
        logging.error(f"Erro ao conectar ou consultar o banco de dados: {e}")

    print(host_sql)
    logging.info(user_sql)
    logging.info(database_sql)
    logging.info(host_sql)
    logging.info(password_sql)


# categoria
@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_categoria(myTimer: func.TimerRequest) -> None:
    # importar variáveis de ambiente
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    # como criar uma connection string usando pyodbc
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        # Criar a conexao com o banco
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # fazer um select * na tabela
            cursor.execute("SELECT * FROM itsm.categoria")
            rows = cursor.fetchall()

            # imprimir os dados da tabela usando logging.info()
            if not rows:
                logging.info("A consulta não retornou nenhum dado.")
            else:
                colunas = [c[0] for c in cursor.description]
                salvar_na_bronze("categoria", colunas, rows)

    except pyodbc.Error as e:
        logging.error(f"Erro ao conectar ou consultar o banco de dados: {e}")

    print(host_sql)
    logging.info(user_sql)
    logging.info(database_sql)
    logging.info(host_sql)
    logging.info(password_sql)


# csat_avaliacao e fila
# Primeira função - duda
@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_csat_avaliacao(myTimer: func.TimerRequest) -> None:

    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM itsm.csat_avaliacao")
            rows = cursor.fetchall()

            if not rows:
                logging.info("A consulta não retornou nenhum dado.")
            else:
                colunas = [c[0] for c in cursor.description]
                salvar_na_bronze("csat_avaliacao", colunas, rows)

    except pyodbc.Error as e:
        logging.error(f"Erro ao conectar ou consultar a tabela csat_avaliacao: {e}")


# Segunda função - duda
@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_fila(myTimer: func.TimerRequest) -> None:

    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM itsm.fila")
            rows = cursor.fetchall()

                        if not rows:
                logging.info("A consulta não retornou nenhum dado.")
            else:
                colunas = [c[0] for c in cursor.description]
                salvar_na_bronze("fila", colunas, rows)

    except pyodbc.Error as e:
        logging.error(f"Erro ao conectar ou consultar a tabela fila: {e}")


# sla
@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_sla(myTimer: func.TimerRequest) -> None:
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM itsm.sla")
            rows = cursor.fetchall()

                       if not rows:
                logging.info("A consulta não retornou nenhum dado.")
            else:
                colunas = [c[0] for c in cursor.description]
                salvar_na_bronze("sla", colunas, rows)

    except pyodbc.Error as e:
        logging.error(f"Erro ao conectar ou consultar a tabela sla: {e}")


# solicitante
@app.timer_trigger(
    schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False
)
def extract_solicitante(myTimer: func.TimerRequest) -> None:
    user_sql = os.getenv("USER")
    database_sql = os.getenv("DATABASE")
    host_sql = os.getenv("HOST")
    password_sql = os.getenv("PASSWORD")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host_sql};"
        f"DATABASE={database_sql};"
        f"UID={user_sql};"
        f"PWD={{{password_sql}}};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM itsm.solicitante")
            rows = cursor.fetchall()

            if not rows:
                logging.info("A consulta não retornou nenhum dado.")
            else:
                colunas = [c[0] for c in cursor.description]
                salvar_na_bronze("solicitante", colunas, rows)

    except pyodbc.Error as e:
        logging.error(f"Erro ao conectar ou consultar a tabela solicitante: {e}")


@app.schedule(schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def timer_trigger_turmac(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Hello Word!")


@app.route(route="http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    name = req.params.get("name")

    if name:
        return func.HttpResponse(
            f"Hello, {name}. This HTTP triggered function executed successfully."
        )
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200,
        )


@app.route(route="http_trigger_v3", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger_v3(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    name = req.params.get("name")
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get("name")

    if name:
        return func.HttpResponse(f"Valor recebido: {name}. - respondido pela http_trigger_v3.")
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200,
        )


@app.schedule(
    schedule="0 */2 * * * *", arg_name="callTimer", run_on_startup=True, use_monitor=False
)
def timer_trigger_chama_http(callTimer: func.TimerRequest) -> None:
    if callTimer.past_due:
        logging.info("O timer (chamador) esta atrasado!")

    base_url = os.environ.get(
        "ECHO_FUNCTION_URL",
        "https://funcapp-giovana001-hzhvefgud6e2hyc6.canadacentral-01.azurewebsites.net/api/http_trigger_v3",
    )
    name = "ola-do-timer"
    url = f"{base_url}?{urllib.parse.urlencode({'name': name})}"

    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            resposta = response.read().decode("utf-8")
            logging.info(f"Resposta da http_trigger_v3: {resposta}")
    except Exception as e:
        logging.error(f"Erro ao chamar a http_trigger_v3: {e}")
