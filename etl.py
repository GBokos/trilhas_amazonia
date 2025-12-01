import logging
import requests
import pandas as pd

from datetime import timedelta
from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.api_core.exceptions import NotFound

# Mapeamento BigQuery → Pandas
mapa_tipos = {
        "STRING": "string",
        "INTEGER": "Int64",
        "INT64": "Int64",
        "FLOAT": "float64",
        "FLOAT64": "float64",
        "NUMERIC": "float64",
        "BOOLEAN": "boolean",
        "BOOL": "boolean",
        "DATE": "datetime64[ns]",
        "DATETIME": "datetime64[ns]",
        "TIMESTAMP": "datetime64[ns]",
        "TIME": "string"
}

# Mapeamento inverso para pandas → BigQuery
mapa_pandas_bq = {
        "object": "STRING",
        "string": "STRING",
        "int64": "INT64",
        "Int64": "INT64",
        "float64": "FLOAT64",
        "bool": "BOOL",
        "boolean": "BOOL",
        "datetime64[ns]": "TIMESTAMP"
}

## Função para buscar dados da API

def buscar_dados(token, api_url):
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        dados = response.json()
        logging.info("Dados obtidos com sucesso.")
        return dados
    else:
        logging.error(f"Erro ao buscar dados: {response.status_code} - {response.text}")
        return None

## Funções ETL

def busca_dados_completos(token, API_URL, bq_client, projeto, aplicativo, categorias_por_aplicativo):
    categorias = categorias_por_aplicativo.get(aplicativo, None)

    for categoria in categorias:
        table_id = projeto[0]+'.'+aplicativo+'.'+categoria.lower()

        print(table_id)
        print("\n")

        df_final = pd.DataFrame()

        skip = 0
        top = 50

        while True:

            url = f"{API_URL}{aplicativo}/{categoria}?$skip={skip}&$top={top}"
            print(url)

            dados = buscar_dados(token, url)

            if not dados or 'value' not in dados or len(dados['value']) == 0:
                print(f"Fim da lista para {categoria} em {projeto}.")
                break

            df = pd.DataFrame(dados['value'])

            if categoria.lower() == "notafiscal":
                df = df.drop(columns=['@odata.type'], errors='ignore')

            if len(df) > 0:
                df_final = pd.concat([df_final, df], ignore_index=True)
            
            print(len(df_final))

            skip+=top

        if df_final.empty:
            print("DataFrame final vazio, nada a carregar.")
        else:
            print(df_final)
            cria_tabela(bq_client, df_final, table_id)

def atualiza_dados(token, API_URL, bq_client, projeto, aplicativo, categorias_por_aplicativo):
    categorias = categorias_por_aplicativo.get(aplicativo, None)

    filtro = "?%24orderby=dataInclusao%20desc"

    for categoria in categorias:
        table_id = projeto+'.'+aplicativo+'.'+categoria.lower()

        query = f"""
            SELECT MAX(dataInclusao) 
            FROM `{table_id}`
        """

        try:
            bq_client.get_table(table_id)
            tabela_existe = True
        except NotFound:
            tabela_existe = False

        if tabela_existe:
            print("\n")
            print(table_id)

            result = bq_client.query(query).result()
            data = next(result)

            data_mais_recente = pd.to_datetime(data[0])

            print(data_mais_recente)

            try:
                dados = buscar_dados(token, API_URL+aplicativo+'/'+categoria+filtro)
                df = pd.DataFrame(dados['value'])

                df['dataInclusao'] = pd.to_datetime(df['dataInclusao'], format="ISO8601", utc=True)

                if categoria.lower() == "notafiscal":
                    df = df.drop(columns='@odata.type')

                df = df[df['dataInclusao'] > data_mais_recente]

                df['dataInclusao'] = pd.to_datetime(df['dataInclusao']).astype(str)
            except Exception as e:
                print('Pulando categoria:', categoria)
                print(f"Erro ao buscar dados: {e}")
                continue
            
        else:
            try:
                print(table_id)

                dados = buscar_dados(token, API_URL+aplicativo+'/'+categoria+filtro)

                df = pd.DataFrame(dados['value'])
            except Exception as e:
                print('Pulando categoria:', categoria)
                print(f"Erro ao buscar dados: {e}")
                continue

        try:
            if df.empty:
                print(f"Nenhum dado novo encontrado para {categoria} em {projeto}.")
            else:
                print(df)
            
            append_dados(bq_client, df, table_id)
        except Exception as e:
            print(f"Erro ao carregar dados: {e}")
            continue

def busca_historico(token, API_URL, bq_client, projeto, aplicativo, categorias_por_aplicativo):
    categorias = categorias_por_aplicativo.get(aplicativo, None)

    filtro = "%24orderby=dataInclusao%20desc"

    for categoria in categorias:
        table_id = projeto+'.'+aplicativo+'.'+categoria.lower()

        print("\n")

        df_final = pd.DataFrame()

        query = f"""
            SELECT MIN(dataInclusao)
            FROM {table_id}
        """

        try:
            bq_client.get_table(table_id)
            tabela_existe = True
        except NotFound:
            tabela_existe = False

        # tabela_existe = False # For testing purposes, we assume the table does not exist
        
        if tabela_existe:
            print("Tabela existe.")
            print(table_id)
            print("\n")

            result = bq_client.query(query).result()
            data = next(result)

            data_mais_antiga = pd.to_datetime(data[0])

            print(data_mais_antiga)

            skip = 0
            top = 50

            while True:

                url = f"{API_URL}{aplicativo}/{categoria}?$skip={skip}&$top={top}&{filtro}"
                print(url)

                dados = buscar_dados(token, url)

                if not dados:
                    break

                df = pd.DataFrame(dados['value'])

                if df.empty:
                    print(f"Fim da lista para {categoria} em {projeto}.")
                    break
                if categoria.lower() == "notafiscal":
                    df = df.drop(columns='@odata.type')

                try:
                    df['dataInclusao'] = pd.to_datetime(df['dataInclusao'], format="ISO8601", utc=True)

                    df_filtrado = df[df['dataInclusao'] < data_mais_antiga]

                    if len(df_filtrado) > 0:
                        df_final = pd.concat([df_final, df_filtrado], ignore_index=True)
                except Exception:
                    break
                print(len(df_final))

                skip+=top

            try:
                print(df_final)

                append_dados(bq_client, df_final, table_id)
            except Exception as e:
                print("Dataframe vazio.")
        else:
            skip = 0
            top = 50

            print(table_id)
            print('Tabela não existe, buscando dados completos.')

            while True:

                url = f"{API_URL}{aplicativo}/{categoria}?$skip={skip}&$top={top}&{filtro}"
                print(url)

                dados = buscar_dados(token, url)

                if not dados or len(dados['value']) == 0:
                    break

                df = pd.DataFrame(dados['value'])

                if categoria.lower() == "notafiscal":
                    df = df.drop(columns='@odata.type')

                df_final = pd.concat([df_final, df], ignore_index=True)
                print(len(df_final))

                skip+=top

            try:
                print(df_final)
                print(len(df_final))

                # df_final.to_csv('df_final_ordem_venda.csv', index=False)

                append_dados(bq_client, df_final, table_id)
            except Exception as e:
                print("Dataframe vazio.")
                raise e

def verifica_alteracoes(token, API_URL, bq_client, bqstorage_client, projeto, aplicativo, categorias_por_aplicativo):
    """
    Função para verificar se houve alterações nos dados e atualizá-los no BigQuery.
    """

    print("\nVerificando alterações para:", aplicativo)

    categorias = categorias_por_aplicativo.get(aplicativo, None)

    filtro = "%24orderby=dataAlteracao%20desc"

    for categoria in categorias:
        print("\nCategoria:", categoria)
        table_id = projeto+'.'+aplicativo+'.'+categoria.lower()

        df_left_final = pd.DataFrame()
        df_final = pd.DataFrame()

        print("\n")

        skip = 0
        top = 50

        # Verifica a data mais recente na tabela
        query = f"""
            SELECT MAX(dataInclusao) 
            FROM {table_id}
        """

        try:
            table = bq_client.get_table(table_id)
            tabela_existe = True
        except NotFound:
            tabela_existe = False
        
        if tabela_existe:
            print(table_id)

            result = bq_client.query(query).result()
            data = next(result)

            data_inclusao_mais_recente = pd.to_datetime(data[0])
            data_alteracao = data_inclusao_mais_recente # Inicializa data_alteracao para entrar no loop

            data_limite = data_inclusao_mais_recente - timedelta(days=200)

            print(f"data_inclusao_mais_recente: {data_inclusao_mais_recente}")
            print(f"data_limite: {data_limite}")

            # Buscar tabela original para comparar alterações
            query = f"""
                SELECT id, dataAlteracao
                FROM {table_id}
                    WHERE dataInclusao >= '{data_limite.strftime('%Y-%m-%d %H:%M:%S')}'
                    QUALIFY row_number() OVER (PARTITION BY id ORDER BY dataAlteracao DESC) = 1
            """

            # df_original = client.query(query).to_dataframe(bqstorage_client=client)
            df_original = bq_client.query(query).to_dataframe(bqstorage_client=bqstorage_client)

            df_original['dataAlteracao'] = pd.to_datetime(df_original['dataAlteracao'], format="ISO8601", utc=True)
            # df_original['dataInclusao'] = pd.to_datetime(df_original['dataInclusao'], format="ISO8601", utc=True)

            while data_alteracao > data_limite:

                url = f"{API_URL}{aplicativo}/{categoria}?$skip={skip}&$top={top}&{filtro}"
                print(url)

                dados = buscar_dados(token, url)

                df = pd.DataFrame(dados['value'])
                if df.empty:
                    print(f"Fim da lista para {categoria} em {projeto}.")
                    break

                if categoria.lower() == "notafiscal":
                    df = df.drop(columns='@odata.type')

                # Filtrando dataframe para considerar apenas registros alterados nos últimos 60 dias
                df['dataAlteracao'] = pd.to_datetime(df['dataAlteracao'], format="ISO8601", utc=True)
                # df['dataInclusao'] = pd.to_datetime(df['dataInclusao'], format="ISO8601", utc=True)

                data_alteracao = df['dataAlteracao'].min()

                df_filtrado = df[df['dataAlteracao'] > data_limite]

                if len(df_filtrado) > 0:
                    # Comparar com df_original e identificar alterações
                    df_merged = df_filtrado.merge(df_original, on='id', suffixes=('_new', '_orig'), how='left', indicator=True)

                    # df_left = df_merged[df_merged['_merge'] == 'left_only']
                    # df_left_final = pd.concat([df_left_final, df_left], ignore_index=True)


                    # print(f"Registros depois do left_only: {len(df_merged[df_merged['_merge'] == 'left_only'])}")
                    # print(f"Registros depois do right_only: {len(df_merged[df_merged['_merge'] == 'right_only'])}")
                    # print(f"Registros antes do both: {len(df_merged)}")
                    # df_merged = df_merged[df_merged['_merge'] == 'both']
                    # print(f"Registros depois do both: {len(df_merged)}")

                    df_merged = (
                        df_merged
                        .sort_values("dataAlteracao_new", ascending=False)
                        .groupby("id", as_index=False)
                        .first()
                    )

                    df_alterados = df_merged[df_merged['dataAlteracao_new'].ne(df_merged['dataAlteracao_orig'])]

                    # Aqui você pode implementar a lógica para atualizar os registros alterados no BigQuery
                    print(f"Registros alterados encontrados: {len(df_alterados)}")

                    if not df_alterados.empty:
                        df_final = pd.concat([df_final, df_alterados], ignore_index=True)
                        
            
                skip+=top
            
            try:
                df_final = df_final.rename(columns={"dataAlteracao_new": "dataAlteracao"})
                df_final = df_final.drop(columns=["dataAlteracao_orig", "_merge"])
            except Exception as e:
                print("Erro ao renomear colunas ou dataframe não criado:", e)
            
            print(f"Tamanho da tabela de registros alterados para a tabela: {len(df_final)}")

            print(df_final)

            if not df_final.empty:
                print("Aplicando tipos...")
                df_final = aplicar_tipos(bq_client, df_final, table_id, table)

            # df_final.to_csv('df_final'+categoria+'.csv', index=False)

            if not df_final.empty:
                try:
                    # append_dados(bq_client, df_final, table_id)
                    print(df_final)
                except Exception as e:
                    print("Dataframe vazio ou erro ao carregar dados:", e)

## Criar e atualizar tabelas no BigQuery

def cria_tabela(client, df, table_id):
    try:
        job = client.load_table_from_dataframe(
                            df,
                            table_id,
                            job_config=bigquery.LoadJobConfig(
                                write_disposition="WRITE_TRUNCATE",
                                autodetect=True
                                )
                        )

        job.result()

        print('Tabela carregada com sucesso.')
    except Exception as e:
        raise e

def append_dados(client, df, table_id):
    try:
        job = client.load_table_from_dataframe(
                            df,
                            table_id,
                            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                        )

        job.result()

        print('Tabela carregada com sucesso.')
    except Exception as e:
        raise e

## Funções para aplicar tipos de dados do BigQuery ao DataFrame

def aplicar_tipos(bq_client, df: pd.DataFrame, table_id, table) -> pd.DataFrame:
    ordem_colunas = [schema_field.name for schema_field in table.schema]
    tipo_colunas = {schema_field.name: schema_field.field_type for schema_field in table.schema}

    colunas_com_erro = {}

    for coluna, tipo_bq in tipo_colunas.items():
        if coluna in df.columns:
            tipo_pd = mapa_tipos.get(tipo_bq)
            if tipo_pd:
                try:
                    df[coluna] = df[coluna].astype(tipo_pd)
                except Exception as e:
                    print(f"⚠️ Erro ao converter coluna '{coluna}' para '{tipo_pd}': {e}")

                    tem_lista = df[coluna].apply(lambda x: isinstance(x, list)).any()

                    if not tem_lista:
                        # Descobre o tipo atual da coluna
                        tipo_atual = str(df[coluna].dtype)
                        tipo_bq_sugerido = mapa_pandas_bq.get(tipo_atual, "STRING")
                        
                        colunas_com_erro[coluna] = tipo_bq_sugerido
                        print(f"➡️ Coluna '{coluna}' será sugerida como '{tipo_bq_sugerido}' (pandas: {tipo_atual})")
    
    if colunas_com_erro:                    
        print(f"Colunas com erro de conversão: {colunas_com_erro}")
        recriar_tabela_com_tipos(bq_client, table_id, table, colunas_com_erro)
    else:
        print("✅ Todas as colunas convertidas com sucesso.")
    
    df = df[ordem_colunas]

    return df

def recriar_tabela_com_tipos(bq_client, table_id: str, table, novos_tipos: dict):
    """
    Recria uma tabela BigQuery aplicando novos tipos de coluna.
    
    Args:
        table_id (str): ID do projeto no BigQuery.
        novos_tipos (dict): Ex: {'coluna1': 'STRING', 'coluna2': 'INT64'}
    """
    
    # Monta SELECT com SAFE_CAST para cada coluna
    colunas_sql = []
    for field in table.schema:
        nome = field.name
        tipo_atual = field.field_type
        tipo_novo = novos_tipos.get(nome, tipo_atual)  # mantém tipo original se não for alterado
        
        if tipo_novo != tipo_atual:
            colunas_sql.append(f"SAFE_CAST(CAST({nome} AS {tipo_novo}) AS {tipo_novo}) AS {nome}")
        else:
            colunas_sql.append(nome)
    
    colunas_sql_str = ",\n  ".join(colunas_sql)

    query = f"""
    CREATE OR REPLACE TABLE `{table_id}` AS
    SELECT
      {colunas_sql_str}
    FROM `{table_id}`;
    """

    print("--> Executando recriação da tabela com novos tipos...")
    print(query)

    job = bq_client.query(query)
    job.result()  # Espera o job terminar
    print(f"✅ Tabela `{table_id}` recriada com sucesso com os novos tipos!")
