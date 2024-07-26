# Projeto de Pesquisa de Preço

Este projeto é responsável por realizar uma pesquisa de preços utilizando dados extraídos de APIs, processá-los e gerar relatórios em formato CSV e Excel. Além disso, o projeto envia esses relatórios por e-mail utilizando o serviço Amazon SES.

## Descrição
O script faz o seguinte:

1. Configuração e Inicialização

    - Define o fuso horário e a data e hora atuais.

    - Configura URLs de APIs e token de autenticação.

2. Funções Principais

    - define_dates(now, specific_time): Define as datas de início e fim para a pesquisa.
        
    - get_list_of_checklist(token, url): Obtém e filtra uma lista de checklists ativos a partir da API.
        
    - get_evaluationid_checklists_aplied(token, url): Obtém uma lista de IDs de avaliação a partir dos checklists aplicados.
        
    - get_checklists_aplied(token, url): Obtém os dados dos checklists aplicados.
        
    - clean_and_convert(value): Limpa e converte valores monetários.

    - send_email(): Envia um e-mail com os relatórios em anexo utilizando Amazon SES.

3. Processamento de Dados

    - Obtém dados de checklists aplicados.

    - Limpa e normaliza os dados.

    - Converte os dados em um DataFrame.

    - Cria relatórios em formato CSV e Excel.

    - Envio de E-mail
    
    - Envia e-mails com os relatórios gerados como anexos utilizando o Amazon SES.

4. Requisitos

    - requests
    
    - pandas
    
    - pytz
    
    - boto3
    
    - python-dotenv
    
    - email
    
## Configuração
    
1. Criação de Arquivo '.env'

    Crie um arquivo '.env' na raiz do projeto com as seguintes variáveis:

```bash
    TOKEN=your_api_token
    SENDER=your_email@example.com
    RECIPIENTS=email1@example.com,email2@example.com
    AWS_ACCESS_KEY_ID=your_aws_access_key_id
    AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
```

2. Instalação de Dependências

```bash
    pip install -r requirements.txt
```