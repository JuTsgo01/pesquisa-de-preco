#%%
import requests
import pandas as pd
from pandas import json_normalize
from datetime import datetime, timedelta, time
import pytz
import json
import os
import boto3

from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import base64

load_dotenv()

timezone = pytz.timezone('America/Sao_Paulo')# Define o fuso horário de SP
now = datetime.now(timezone)# Data e hora atuais no fuso horário especificado
specific_time = time(3, 0, 0)# Define a hora específica (3 da manhã)

url_base_integration = 'https://integration.checklistfacil.com.br/'
url_base_analytics = 'https://api-analytics.checklistfacil.com.br/'
token = os.getenv('TOKEN')

def define_dates(now, specific_time):
    today_at_specific_time = datetime.combine(now.date(), specific_time, tzinfo=timezone)
    start_at = today_at_specific_time - timedelta(days=34) #data inicial formata no fuso horário especificado (ISO 8601)
    end_at = today_at_specific_time - timedelta(days=31)# data final formata no fuso horário especificado (ISO 8601)
    start_at_formatada = (now - timedelta(days=34)).strftime('%Y-%m-%d')# Data inicial com formatão básica para usar na coluna
    end_at_formatada = (now - timedelta(days=32)).strftime('%Y-%m-%d') # Data final com formatão básica para usar na coluna
    return start_at.isoformat(), end_at.isoformat(), start_at_formatada, end_at_formatada #return das datas

def get_list_of_checklist(token: str, url: str):
    
    """
    Obtém e filtra uma lista de checklists a partir de uma API.

    Parâmetros:
    - token (str): Token de autenticação para acessar a API.
    - url (str): URL da API para obter a lista de checklists.

    Retorna:
    - pd.DataFrame: DataFrame contendo os checklists ativos e não deletados.
    - None: Em caso de erro na requisição.
    """
    
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)
        data = response.json().get('data', [])
        list_of_checklist_df = pd.DataFrame(data)
        list_of_checklist_df = list_of_checklist_df[(list_of_checklist_df['active']) & (list_of_checklist_df['deletedAt'].isnull())]
        return list_of_checklist_df
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição: {e}")
        return None
    
def get_evaluationid_checklists_aplied(token: str, url: str):
    
    """
    Obtém uma lista de evaluationIds a partir dos checklists aplicados.

    Parâmetros:
    - token (str): Token de autenticação para acessar a API.
    - url (str): URL da API para obter a lista de evaluationIds.

    Retorna:
    - list: Lista de evaluationIds únicos.
    - str: Mensagem de erro em caso de falha na requisição.
    """
    
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)
        data = response.json().get('data', [])
        df_evaluation_checklists_aplied = pd.DataFrame(data)
        evaluationId = df_evaluation_checklists_aplied['evaluationId'].unique()
        return evaluationId
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição: {e}")
        return None
    
def get_checklists_aplied(token: str, url: str):
    
    """
    Obtém os dados de checklist aplicados a partir dos evaluationIds da função anterior
    'get_evaluationid_checklists_aplied'.
    
    Aqui, pra cada evaluationIds, atraves do for pegamos cada checklist aplicado.
    Os checklists aplicados gerará um json com todos os dados dos checklists.
    Para conseguirmos visualizar acad um dos dados, vamos explordir e normalizar o dataframe
    
    Parâmetros:
    - token (str): Token de autenticação para acessar a API.
    - url (str): URL da API para obter os dados do checklist.

    Retorna:
    - dict: Dados do checklist aplicados.
    - str: Mensagem de erro em caso de falha na requisição.
    """
    
    try:
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            return f"Error: {response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"Error: {e}"
    
def clean_and_convert(value):
    
    '''
    Essa função tem como objetivo tratar números que por ventura tenha mais que um ponto decimal
    
    Queremos apenas número no estilo: "R$ xxx.xx" ou "R$ xxx,xx"
    
    Exemplo:
    Temos um número R$1.001.00
    
    1 - passo da função: susbbstituir o "R$" por ""
    Resultado do primeiro passo no nosso exemplo: "1.000.00"
    
    2 - passo da função: substituir  ","  por "."
    Resultado do segundo passo no nosso exemplo: "1.000.00"
    Não houve mudança por que não temos "," no número
    
    3 - passo da função: Tratar justamento caso de mais de um ponto decimal
    Como: Ele a função fará a contagem de quantos "." decimal tem no objeto e depois separar em partes,
    primeira parte antes do primeiro "." decimal, segunda parte entre os "." decimal e a terceira parte 
    depois do segundo "." decimal
    
    Resultado do terceiro passo no nosso exemplo: "1", "000" e "00"
    
    4 - passo da função: Concatenar as partes separadas e mantendo apenas o ultimo decimal
    Resultado do quarto passo no nosso exemplo: "1" + "000" e depois a ultima parte ".00"
    resultando em "1000" + ".00" = "1000.00" e convertendo para float
    
    '''
    
    if not isinstance(value, str): # Estamos verificando se o valor é uma string
        return None  # Caso o 'value' não seja uma string, a condição retorna none algum valor padrão como 0.0 ou np.nan
    cleaned_value = value.replace('R$ ', '').replace(',', '.')# Remove 'R$' e substitui vígula por ponto
    
    '''
    cleaned_value = ''.join(c for c in cleaned_value if c.isdigit() or c == '.')
    
    Olha para cada caracter e verifica se é um digito ou um ponto decimal. Se for ponto decimal ou digito,
    ele será mantido na nova string final para depois ser convertido para float.
    
    Caso não seja um digito ou um ponto decimal, ele será removido da nova string final e manterá apenas se 
    for um digito ou um ponto decimal.
    
    EX: 13I,75
    Resultado da nova string final: '13.75'
    Explicação: Removeu a "," por "." e o 'I' por '' ejuntou em uma string final
    '''
    cleaned_value = ''.join(c for c in cleaned_value if c.isdigit() or c == '.')
    
    # Tratar caso de mais de um ponto decimal
    if cleaned_value.count('.') > 1: # Verifica se ha mais de um ponto decimal
        parts = cleaned_value.split('.') #Dividi o valor em partes antes e depois do ponto decimal
        #Juntando as partes antes e depois do ponto decimal, pegando apenas a ultima parte
        cleaned_value = ''.join(parts[:-1]) + '.' + parts[-1] 
    
    try:
        return float(cleaned_value) #Retorna um float se as condições satisfazerem
    except ValueError:
        return None  # Ou algum valor padrão como 0.0 ou np.nan caso der um Erro de valor
    
def send_email():
    sender = os.getenv('SENDER')
    recipients = os.getenv('RECIPIENTS')
    subject = "Teste - Pesquisa de preço do último fim de semana em arquivo CSV e EXCEL"
    
    # Os arquivos que você deseja anexar
    attachments = [f"resultado_pesquisa_preco_{start_at_formatada}_{end_at_formatada}.csv", f"resultado_pesquisa_preco_{start_at_formatada}_{end_at_formatada}.xlsx"]

    # Construa a mensagem do e-mail
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    # Anexar os arquivos
    for attachment in attachments:
        with open(attachment, "rb") as file:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {os.path.basename(attachment)}",
            )
            msg.attach(part)
    
    try:
        response = ses_client.send_raw_email(
            Source=sender,
            Destinations=recipients,
            RawMessage={
                'Data': msg.as_string(),
            },
        )
        print("Email sent! Message ID:", response['MessageId'])
    except NoCredentialsError:
        print("Credentials not available.")
    except PartialCredentialsError:
        print("Credenciais fornecidas estão incompletas.")
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")    
   
if __name__ == "__main__":
    
    start_at_str, end_at_str, start_at_formatada, end_at_formatada = define_dates(now, specific_time)
    
    list_of_checklist_active = get_list_of_checklist(token, f'{url_base_analytics}v1/checklists?{1}&True&200')

    evaluationIds = get_evaluationid_checklists_aplied(token,
    f'{url_base_analytics}v1/evaluations?startedAt[gte]={start_at_str}&concludedAt[lte]={end_at_str}&{2}&checklistId=248447&{10}&{1000}')
    price_research = [] 

    for evaluationId in evaluationIds:
        result = get_checklists_aplied(token, f'{url_base_integration}v2/evaluations/{evaluationId}')
        if isinstance(result, dict):
            price_research.append(result)
        else:
            print(result)
    
    df_price_research = pd.DataFrame(price_research)

    columns_to_drop = ['id','score', 'status', 'classification',
                    'schedule','perimeter', 'checklist',
                    'startedAt', 'concludedAt', 'approvedAt',
                    'platform', 'departments', 'attachments',
                    'signatures', 'finalComment', 'sharedTo']


    df_price_research = df_price_research.copy()

    #Esse '.copy()' é para previnir 'SettingWithCopyWarning'
    #Estamos usando a cópia apenas para dropar as colunas da cópia e não do df original

    df_price_research = df_price_research.drop(columns=columns_to_drop)

    
    columns_to_name = ['unit', 'user']

    for column_to_name in columns_to_name:
        df_price_research[column_to_name] = df_price_research[column_to_name].apply(lambda x: x['name'])

    
    df_price_research_exploding = df_price_research.explode('categories')

    # Expandindo a coluna 'categories' para depois normalizar os items que estava dentro da chave e valor
    df_expanded_categories = pd.json_normalize(df_price_research_exploding['categories'])
    df_expanded_items = df_expanded_categories.explode('items')
    df_expanded_items_normalized = pd.json_normalize(df_expanded_items['items'])

    # Usando a coluna original para depois repetir os valores conforme o 'exploding'
    original_columns = df_price_research_exploding.drop(columns=['categories']).columns

    for col in original_columns:
        repeated_values = df_price_research_exploding[col].repeat(df_expanded_items.groupby(df_expanded_items.index).size())
        df_expanded_items_normalized[col] = repeated_values.values

    # Filtrar linhas onde a coluna 'scale' é igual a 5 (Filtro apenas de produto e ,consequentemente, valores. Ou seja, sem data, gps e etc)
    df_filtered = df_expanded_items_normalized[df_expanded_items_normalized['scale'] == 5]

    columns_to_remove = ['scale', 'weight', 'currency', 'comment',
                        'attachments', 'signatures', 'answer.answeredAt',
                        'answer.evaluative', 'answer.number', 'answer.state',
                        'answer.city', 'answer.product', 'answer.competencePeriod',
                        'answer.selectedOptions', 'answer.index',
                        'answer.mathOperation', 'dependencies',
                        'weight.original', 'weight.obtained',
                        'weight.maximum']


    df_filtered = df_filtered.copy() 

    '''

    Esse '.copy()' é para previnir 'SettingWithCopyWarning'
    Estamos usando a cópia apenas para dropar as colunas da cópia e não do df original
    Logo em seguida estamos dropando

    '''

    df_filtered = df_filtered.drop(columns=columns_to_remove)


    #Removendo alguns valores indesejados das linhas dos produtos
    df_filtered['name'] = df_filtered['name'].str.replace(r' - Informe o valor e anexe a foto;?:?', '', regex=True)

    df_filtered['answer.text'] = df_filtered['answer.text'].apply(clean_and_convert)

    #Renomeando as colunas para ficar mais legivel ao analisar as planilhas
    df_filtered.rename(columns={'name': 'produto', 'unit': 'unidade',
                                'user': 'responsável', 'answer.text': 'valor'}, inplace=True)

    #Aqui criei um dicionário para renomear as lojas
    store_mapping = {
        'Araraquara Centro': 1,
        'Ribeirão Preto': 2,
        'Piracicaba': 3,
        'Rio Preto': 4,
        'Jundiaí': 5,
        'SP Moema': 6,
        'SP Tatuapé': 7,
        'Campinas Cambuí': 8,
        'Campinas Norte-Sul Drive': 9,
        'SP Palestra Itália': 10,
        'SP Santana': 11,
        'Curitiba Batel': 12,
        'SP Grajaú': 13,
        'SP Americanópolis': 14,
        'Ribeirão Preto Drive': 15,
        'Araraquara Carmo': 16,
        'Catanduva': 17,
        'Barretos': 18,
        'Bebedouro': 19,
        'Sao Jose dos Campos': 20,
        'Uberaba': 21,
        'Goiania 01 - CO': 22,
        'Goiania 02 - CO': 23,
        'Aparecida de Goiania 01 - CO': 24,
        'Matão': 25,
        'Franca': 26,
        'Ponta Grossa': 27,
        'Juquehy': 28,
        'Goiania 03 - CO': 29,
        'Aparecida de Goiania 02 - CO': 30,
        'Uberlandia': 31,
        'Anapolis - CO': 32,
        'Curitiba Juvevê': 33,
        'Cuiaba - CO': 34,
        'Campo Grande - CO': 35,
        'Campo Grande 02 - CO': 36,
        'Cuiaba 02 - CO': 37,
        'Águas Claras - CO': 38,
        'Recreio': 39,
        'Americana': 40,
        'São Carlos': 41,
        'Santos': 42,
        'Samambaia - CO': 43,
        'Ceilandia - CO': 44,
        'Rio Verde - CO': 45,
        'Savassi': 46,
        'Buritis': 47,
        'Goiania 04 - CO': 48,
        'Morumbi': 49,
        'Pampulha': 50,
        'Rio Vermelho': 51,
        'Barra': 52,
        'Pituba': 53,
        'Goiania 05 - CO': 54,
        'Goiania 06 - CO': 55,
        'Guarulhos': 56,
        'Copacabana': 57,
        'Shopping Jaraguá': 58,
        'Boa Viagem': 59,
        'Meireles': 60,
        'Espinheiro': 61,
        'São Luis 02': 62,
        'Fortaleza 02': 63,
        'São Luis 01': 64,
        'Praia Grande': 65,
        'São Luis 03': 66,
        'Piedade': 67,
        'Fortaleza 03': 68,
        'Varzea Paulista': 69,
        'Jacarepaguá': 70,
        'Autódromo': 71,
        'Belém 01': 72,
        'Belém 02': 73,
        'SP Saúde': 74
    }

    product_dict = {
        "Amstel 600ml": "A_600",
        "Amstel LN 355ml": "A_LN",
        "Amstel LT 269ml": "A_LT269",
        "Amstel Lt 350ml": "A_LT350",
        "Amstel LT 350ml.": "A_LT350",
        "Amstel LT 473ml": "A_LT473",
        "Amstel Ultra LN": "A_ULT_LN",
        "Amstel Ultra LT": "A_ULT_LT",
        "Amstel Vibes 269ml": "A_VIBES",
        "Bavaria LT 350ml": "B_LT350",
        "Bavaria LT 350ml.": "B_LT350",
        "Bavaria LT 473ml": "B_LT473",
        "Bavaria LT 473ml.": "B_LT473",
        "Coca-Cola 2Ltrs ": "C_2L",
        "Coca-Cola 2Ltrs.": "C_2L",
        "Coca-Cola LT 350ml": "C_LT350",
        "Coca-Cola LT 350ml ": "C_LT350",
        "Devassa LT 269ml": "D_LT269",
        "Devassa LT 350ml": "D_LT350",
        "Devassa LT 350ml.": "D_LT350",
        "Devassa LT 473ml": "D_LT473",
        "Eisenbahn 600ml": "E_600",
        "Eisenbahn 600ml.": "E_600",
        "Eisenbahn LN": "E_LN330",
        "Eisenbahn LT 269ml": "E_LT269",
        "Eisenbahn LT 350ml": "E_LT350",
        "Eisenbahn LT 350ml.": "E_LT350",
        "Eisenbahn LT 473ml": "E_LT473",
        "Heineken 600ml - Descartável": "H_600D",
        "Heineken 600ml - Descartável.": "H_600D",
        "Heineken 600ml - Retornável": "H_600R",
        "Heineken 600ml - Retornável.": "H_600R",
        "Heineken barril (5Ltrs)": "H_5L",
        "Heineken barril (5Ltrs).": "H_5L",
        "Heineken LN": "H_LN330",
        "Heineken LN 250ml": "H_LN250",
        "Heineken LN 250ml.": "H_LN250",
        "Heineken LT": "H_LT350",
        "Heineken LT 269": "H_LT269",
        "Heineken LT 473ml ": "H_LT473",
        "Heineken LT 473ml.": "H_LT473",
        "Kaiser LT 350ml": "K_LT350",
        "Kaiser LT 350ml.": "K_LT350",
        "Kaiser LT 473ml": "K_LT473",
        "Kaiser LT 473ml.": "K_LT473",
        "Red Bull 250ml": "RB_250",
        "Red Bull 250ml.": "RB_250",
        "Red Bull 355ml": "RB_355",
        "Red Bull 355ml.": "RB_355",
        "Red Bull 473ml": "RB_473",
        "Red Bull 473ml.": "RB_473",
        "Sol LN": "S_LN330",
        "Tiger LT 350ml": "T_LT350",
        "Tiger LT 350ml.": "T_LT350"
    }

    #Aplicando o dicionário na coluna onde quero alterar o nome do prdoduto
    df_filtered['produto'] = df_filtered['produto'].replace(product_dict)

    #Aplicando o dicionário na coluna onde quero alterar o nome por número da loja
    df_filtered['unidade'] = df_filtered['unidade'].replace(store_mapping)

    #Filtrando a coluna valor para que não tenha valor "0,01" (que são valores inexistentes)
    df_filtered_value = df_filtered[(df_filtered['valor'] > 2) & (df_filtered['valor'] < 201)].copy()

    #Fazendo o pivot do Df para ver colunas de produtos de acordo com as lojas e a média dos valores
    df_pivot = df_filtered_value.pivot_table(index='unidade', columns='produto', values='valor')
    df_pivot['inicio'] = start_at_formatada #Data inicial aidicionando na coluna 'inicio' no Df
    df_pivot['fim'] = end_at_formatada #Data final aidicionando na coluna 'fim' no Df
    df_pivot.fillna("NULL", inplace=True) #No lugar de 'NaN' colocar 'NULL' no Df p/ subir no banco

    #Criando a ordem que eu quero nas colunas do df
    columns = ['inicio', 'fim', 'A_600', 'A_LN', 'A_LT269', 'A_LT350', 
            'A_LT473', 'A_ULT_LN', 'A_ULT_LT', 'A_VIBES', 'B_LT350', 'B_LT473',
            'C_2L', 'C_LT350', 'D_LT269', 'D_LT350', 'D_LT473', 'E_600', 'E_LN330',
            'E_LT269', 'E_LT350', 'E_LT473', 'H_5L', 'H_600D', 'H_600R', 'H_LN250',
            'H_LN330', 'H_LT269', 'H_LT350', 'H_LT473', 'K_LT350', 'K_LT473',
            'RB_250', 'RB_355', 'RB_473', 'S_LN330', 'T_LT350']

    #Aplicando a ordem das colunas no df
    df_pivot = df_pivot[columns]
    
    csv_file = f"resultado_pesquisa_preco_{start_at_formatada}_{end_at_formatada}.csv"
    excel_file = f"resultado_pesquisa_preco_{start_at_formatada}_{end_at_formatada}.xlsx"
    df_pivot.to_csv(csv_file, index=True, header=False)
    df_pivot.to_excel(excel_file, index=True)

    

    # Crie o cliente SES
    ses_client = boto3.client(
        'ses',
        region_name='sa-east-1',
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    
    send_email()
