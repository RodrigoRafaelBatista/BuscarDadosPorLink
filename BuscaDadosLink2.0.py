import requests
import csv
from lxml import html 
import os
import datetime
import time
import threading
import math

inicio_execucao = "{:.2f}".format(time.time())
importa = open('Casa dos Dados/LINKS CSV/LINKS_SAO_LEOPOLDO.csv') # Faz a importação dos links que serão acessados do arquivo CSV
nomeSaida = 'DADOS SLEO.csv'   # Define o nome do arquivo de saída #### Lembrar de por a extensão .CSV####
linhas = csv.reader(importa)    # Recebe as linhas do CSV na variável linhas            
linhas = list(linhas)   # Converte as linhas em uma lista, o que vai permitir consultar pela posição dos elementos
totalLinhas = len(list(linhas))   # Total de linhas da lista
dados = [] # Cria a lista com os dados que serão gerados com as consultas
lista_threads_finalizadas = []  # Lista que salva cada thread que já foi finalizada 

totalThreads = 100   # Definir o total de threads que serão usdas para fazer as consultas


def criar_listas_de_busca():
    if totalThreads > len(linhas):
        print("Número de threads é maior que a quantidade de linhas, encerrando programa...")
        exit()
    lista = {}
    inicio_pesquisa = 0
    fim_pesquisa = 0
    for i in range(0, totalThreads):
        pesquisa_por_thread = totalLinhas / totalThreads

        if (i * pesquisa_por_thread <=0):
            inicio_pesquisa = int(math.ceil((i * pesquisa_por_thread)))
            fim_pesquisa    = int(math.ceil((i + 1) * pesquisa_por_thread))
        else:
            inicio_pesquisa = int(math.ceil(i * pesquisa_por_thread + 1))
            fim_pesquisa    = int(math.ceil((i + 1) * pesquisa_por_thread))

        lista[f'pesquisa{i}'] = [inicio_pesquisa, fim_pesquisa]

    #print(lista)
    return lista #, inicio_pesquisa, fim_pesquisa

def salvar_dados(all_data):
    with open(nomeSaida, mode='a', newline='', encoding='UTF-8') as saida:
        d=['CNPJ',
            'RazaoSocial',
            'NomeFantasia',
            'Tipo',
            'DataAbertura',
            'SituacaoCadastral',
            'DataSituacaoCadastral',
            'CapitalSocial',
            'NaturezaJuridica',
            'EmpresaMEI',
            'Logradouro',
            'Numero',
            'Complemento',
            'CEP',
            'Bairro',
            'Municipio',
            'UF',
            'Telefone1',
            'Telefone2',
            'Telefone3',
            'Telefone4',
            'Email',
            'AtividadePrincipal'
            ]
        escritor_csv = csv.DictWriter(saida, fieldnames=d)
        escritor_csv.writeheader()
        for linha in all_data:
            escritor_csv.writerow(linha)

    return

def buscar_dados(inicio_busca, fim_busca):
    global dados
    for i in range(inicio_busca, fim_busca+1):

        linha = "".join(linhas[i])
        page = requests.get(linha, headers = {'User-Agent': 'Mozilla/5.0'})

        while page.status_code != 200:
            print("Status code foi diferente de 200, tentando novamente...")
            page = requests.get(linha, headers = {'User-Agent': 'Mozilla/5.0'})
            time.sleep(1)
        
        tree = html.fromstring(page.content)  

        verificaNomeFantasiaExiste =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[3]/p[2]/text()'))

        if(verificaNomeFantasiaExiste == "MATRIZ"):
            cnpj                    =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[1]/p[2]/text()'))
            razaoSocial             =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[2]/p[2]/text()'))
            nomeFantasia            =   "VAZIO"
            tipo                    =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[3]/p[2]/text()'))
            dataAbertura            =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[4]/a/text()'))
            situacaoCadastral       =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[5]/p[2]/text()'))
            dataSituacaoCadastral   =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[6]/p[2]/text()'))
            capitalSocial           =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[7]/p[2]/text()'))
            naturezaJuridica        =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[8]/p[2]/text()'))
            empresaMEI              =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[9]/p[2]/text()'))
            logradouro              =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[1]/p[2]/text()'))
            numero                  =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[2]/p[2]/text()'))
            complemento             =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[3]/p[2]/text()'))
            cep                     =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[4]/p[2]/text()'))
            bairro                  =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[5]/p[2]/text()'))
            municipio               =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[6]/p[2]/a/text()'))
            uf                      =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[7]/p[2]/a/text()'))
            telefone1               =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[3]/div[1]/p[2]/a/text()'))
            telefone2               =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[3]/div[1]/p[3]/a/text()'))
            telefone3               =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[3]/div[1]/p[4]/a/text()'))
            telefone4               =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[3]/div[1]/p[5]/a/text()'))
            email                   =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[3]/div[2]/p[2]/a/text()'))
            atividadePrincipal      =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[5]/div[1]/p[2]/text()'))
        else:
            cnpj                    =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[1]/p[2]/text()'))
            razaoSocial             =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[2]/p[2]/text()'))
            nomeFantasia            =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[3]/p[2]/text()'))
            tipo                    =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[4]/p[2]/text()'))
            dataAbertura            =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[5]/a/text()'))
            situacaoCadastral       =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[6]/p[2]/text()'))
            dataSituacaoCadastral   =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[7]/p[2]/text()'))
            capitalSocial           =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[8]/p[2]/text()'))
            naturezaJuridica        =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[9]/p[2]/text()'))
            empresaMEI              =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[1]/div[10]/p[2]/text()'))
            logradouro              =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[1]/p[2]/text()'))
            numero                  =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[2]/p[2]/text()'))
            complemento             =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[3]/p[2]/text()'))
            cep                     =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[4]/p[2]/text()'))
            bairro                  =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[5]/p[2]/text()'))
            municipio               =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[6]/p[2]/a/text()'))
            uf                      =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[2]/div[7]/p[2]/a/text()'))
            telefone1               =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[3]/div[1]/p[2]/a/text()'))
            telefone2               =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[3]/div[1]/p[3]/a/text()'))
            telefone3               =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[3]/div[1]/p[4]/a/text()'))
            telefone4               =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[3]/div[1]/p[5]/a/text()'))
            email                   =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[4]/div[1]/div[3]/div[2]/p[2]/a/text()'))
            atividadePrincipal      =   "".join(tree.xpath('/html/body/div/div/div/div[2]/section[1]/div/div/div[5]/div[1]/p[2]/text()'))

        d = {   'CNPJ':                     cnpj,
                'RazaoSocial':              razaoSocial,
                'NomeFantasia':             nomeFantasia,
                'Tipo':                     tipo,
                'DataAbertura':             dataAbertura,
                'SituacaoCadastral':        situacaoCadastral,
                'DataSituacaoCadastral':    dataSituacaoCadastral,
                'CapitalSocial':            capitalSocial,
                'NaturezaJuridica':         naturezaJuridica,
                'EmpresaMEI':               empresaMEI,
                'Logradouro':               logradouro,
                'Numero':                   numero,
                'Complemento':              complemento,
                'CEP':                      cep,
                'Bairro':                   bairro,
                'Municipio':                municipio,
                'UF':                       uf,
                'Telefone1':                telefone1,
                'Telefone2':                telefone2,
                'Telefone3':                telefone3,
                'Telefone4':                telefone4,
                'Email':                    email,
                'AtividadePrincipal':       atividadePrincipal
            }
        dados.append(d)
        porcentagem ="{:.2f}".format(float(len(dados))/int(len(linhas))*100)
        
        print(f"Extraindo nº {len(dados)} / {len(linhas)}  {porcentagem}%")


    return dados


def criar_threads():
    # Cria as threads e as variaveis
    for i in range(0, totalThreads):
        #print(lista[f'pesquisa{i}'][0], lista[f'pesquisa{i}'][1])
        varThread = "t" + str(i)
        globals()[varThread] = threading.Thread(target=buscar_dados, args=(int(lista[f'pesquisa{i}'][0]), int(lista[f'pesquisa{i}'][1])))
    return

def iniciliza_threads():
    # Inicializa cada thread criada
    for i in range(0, totalThreads):
        varThread = "t" + str(i)
        globals()[varThread].start()
        print(f"A thread {varThread} foi iniciada com sucesso!")
    return

def aguardar_threads_finalizar():
    conta_threads_finalizadas = 0   # Conta a quantida de threads que forem finalizadas
    while True:
        time.sleep(1)
        for i in range(0, totalThreads):
            varThread = "t" + str(i)
            if ( not globals()[varThread].is_alive()):
                if globals()[varThread] not in lista_threads_finalizadas:
                    lista_threads_finalizadas.append(globals()[varThread])
                    print(f"Thread {varThread} foi finalizada")
                    conta_threads_finalizadas += 1

        if(conta_threads_finalizadas == totalThreads):
            print("Todas as threads encerradas, salvando dados no CSV...")
            salvar_dados(dados)
            break
    return

def tempo_total_execucao():
    tempoTotal = (float(fim_execucao) - float(inicio_execucao))
    return ("{:.2f}".format(tempoTotal))

lista = criar_listas_de_busca()
criar_threads()
iniciliza_threads()
aguardar_threads_finalizar()
fim_execucao = "{:.2f}".format(time.time())
print(f"Programa executado em {tempo_total_execucao()} segundos.")
#print(f"Intervalos pesquisados: {lista}")