import pandas as pd
import os
import json
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm  # Biblioteca para barra de progresso

# Função para carregar o HTML com tentativas múltiplas
def load_html(url, retries=3, delay=5):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.text
            else:
                print(f'Tentativa {attempt + 1} falhou com status code {response.status_code}')
        except requests.RequestException as e:
            print(f'Tentativa {attempt + 1} falhou com exceção: {e}')
        time.sleep(delay)
    return None

# Função para salvar dados em um arquivo Excel
def save_to_excel(data, filename):
    data.to_excel(filename, index=False, engine='openpyxl')
    print(f'Dados salvos no arquivo {filename}')

# Função principal de extração
def process_url(row, output_directory):
    vPaginas = row['Paginas']
    
    # Log da URL atual
    print(f'Processando URL: {vPaginas}')
    
    # Carregar o HTML da página
    html = load_html(vPaginas)
    
    if html:
        # HTML carregado com sucesso
        print(f'HTML carregado com sucesso para URL: {vPaginas}')
        
        # Parsing do HTML
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extração do JSON a partir do HTML
        script_tag = soup.find('script', {'type': 'application/ld+json'})
        
        if script_tag:
            try:
                json_data = script_tag.string
                json_obj = json.loads(json_data)
                print(f'JSON extraído com sucesso para URL: {vPaginas}')
                
                # Extração dos dados necessários
                product = json_obj
                offers = product.get('offers', {})
                if isinstance(offers, list):
                    offers = offers[0]  # Caso 'offers' seja uma lista, pegar o primeiro elemento
                
                apresentacao = product.get('name', '')
                ean = offers.get('sku', '')
                ofertas = offers.get('offerCount', '')
                menor_preco = offers.get('lowPrice', '').replace('.', ',')
                maior_preco = offers.get('highPrice', '').replace('.', ',')
                data_preco = offers.get('priceValidUntil', '')
                lojas = json.dumps(offers.get('offers', [])) if 'offers' in offers else ''  # Converter lista de ofertas para string
                
                # Adicionar os dados extraídos à lista
                extracted_data = [{
                    'Apresentacao': apresentacao,
                    'EAN': ean,
                    'Ofertas': ofertas,
                    'Menor_Preco': menor_preco,
                    'Maior_Preco': maior_preco,
                    'Data_Preco': data_preco,
                    'Lojas': lojas,
                    'Json': json_data
                }]
                
                # Converter os dados extraídos em um DataFrame
                df = pd.DataFrame(extracted_data)
                
                # Definir o nome do arquivo com base no EAN
                filename = os.path.join(output_directory, f'ExtracaoSite_{ean}.xlsx')
                
                # Salvar os dados no arquivo Excel
                save_to_excel(df, filename)
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                print(f'Falha ao extrair JSON para URL {vPaginas}: {e}')
        else:
            print(f'Nenhuma tag <script> correspondente encontrada para URL: {vPaginas}')
    else:
        print(f'Falha ao carregar HTML para URL: {vPaginas}')
    
    # Log de progresso
    print(f'Processamento concluído para URL: {vPaginas}')

# Função para juntar todos os arquivos Excel em um único arquivo
def merge_excels(output_directory, final_filename):
    # Lista para armazenar todos os DataFrames
    all_dataframes = []
    
    # Percorrer todos os arquivos no diretório
    for file in os.listdir(output_directory):
        if file.endswith('.xlsx'):
            file_path = os.path.join(output_directory, file)
            # Carregar o arquivo Excel em um DataFrame
            df = pd.read_excel(file_path, engine='openpyxl')
            all_dataframes.append(df)
    
    # Concatenar todos os DataFrames em um único DataFrame
    if all_dataframes:
        merged_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Salvar o DataFrame final em um único arquivo Excel
        final_file_path = os.path.join(output_directory, final_filename)
        merged_df.to_excel(final_file_path, index=False, engine='openpyxl')
        print(f'Todos os dados foram combinados e salvos no arquivo {final_file_path}')
    else:
        print('Nenhum arquivo Excel encontrado para combinar.')

# Carregar URLs a partir do arquivo CSV
url_data = pd.read_csv('D:\\WebScraping\\CliqueFarma\\links\\PaginasCliquefarma202407.csv', delimiter=';', encoding='utf-8')

# Filtrar URLs com status 'Buscar'
url_data = url_data[url_data['Status'] == 'Buscar']

# Verificar se URLs foram carregadas corretamente
print(f'URLs carregadas: {len(url_data)}')

# Definir o diretório onde os arquivos serão salvos
output_directory = 'D:\\WebScraping\\CliqueFarma\\links\\'

# Criar o diretório se não existir
os.makedirs(output_directory, exist_ok=True)

# Utilizar ThreadPoolExecutor para paralelizar o processamento
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = []
    
    # Usar tqdm para mostrar o progresso
    with tqdm(total=len(url_data)) as pbar:
        for _, row in url_data.iterrows():
            future = executor.submit(process_url, row, output_directory)
            futures.append(future)
        
        for future in as_completed(futures):
            pbar.update(1)  # Atualizar a barra de progresso a cada conclusão
            try:
                future.result()
            except Exception as e:
                print(f'Erro ao processar: {e}')

# Juntar todos os arquivos Excel gerados em um único arquivo
merge_excels(output_directory, 'ExtracaoSite_Final.xlsx')

print('Processamento concluído e arquivos mesclados')
