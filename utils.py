import logging
import pandas as pd

from datetime import datetime

def salvar_csv(dados, nome_base='relatorio'):
    data_hoje = datetime.today().strftime('%Y-%m-%d')
    try:
        df = pd.DataFrame(dados['value'])
        
        df.to_csv(f'{nome_base}_{data_hoje}.csv', index=False, encoding="utf-8")
        print("✅ CSV criado com sucesso.")
    except Exception as e:
        print("❌ Erro ao converter:", e)

    print(df)

    logging.info(f'Dados salvos em {nome_base}_{data_hoje}.csv')