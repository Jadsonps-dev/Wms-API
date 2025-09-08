import os
import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
from nbclient import NotebookClient
from bs4 import BeautifulSoup
import time
import warnings
import datetime
import gspread
import requests
import json
from oauth2client.service_account import ServiceAccountCredentials
import sys
import nbformat
import sqlite3

warnings.filterwarnings("ignore")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import *

class Estrutura:
    def __init__(self):

        self.google_drive_path = self.get_google_drive_path()

        self.link_wms = LINK_WMS
        self.user_wms = LOGINS_WMS[0]
        self.senha_wms = SENHAS_WMS[0]
        self.id_token_wms = ID_TOKEN_WMS[0]
        self.token_senha_wms = TOKENS_SENHAS[0]
    
    def get_google_drive_path(self):
        if os.name == 'nt':
            possible_paths = [r"G:\Meu Drive", r"H:\Meu Drive"]
            for path in possible_paths:
                if os.path.exists(path):
                    self.drive_path = path
                    return self.drive_path
            print("Nenhum dos caminhos do Google Drive foi encontrado ou está acessível no Windows.")
            return None
        else:
            self.drive_path = r"/home/luftsolutions/GoogleDrive"
            if os.path.exists(self.drive_path):
                return self.drive_path
            else:
                print(f"O caminho {self.drive_path} não foi encontrado no Linux.")
                return None
    
    def login_wms(self, nome_usuario, senha_usuario, id_armazem=7):
        login_url = self.link_wms + r'webresources/SessionService/login'

        descricoes_armazem = {
            7: "LUFT SOLUTIONS - AG2 - CAJAMAR - 16",
            8: "LUFT - ARMAZÉM - MARABRAZ - 21"
        }

        descricao = descricoes_armazem.get(id_armazem, "DESCONHECIDO")

        login_data = {
            "nomeUsuario": nome_usuario,
            "password": senha_usuario,
            "armazem": {
                "id": id_armazem,
                "descricao": descricao
            }
        }

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

        self.session = requests.Session()
        self.session.get(self.link_wms, headers=self.headers)
        response = self.session.post(login_url, json=login_data, headers=self.headers)

        return response
    
    def baixar_csv_wms(self, file_name, save_path, nome_arquivo=None, id_relatorio=None):
        file_url = f"http://200.143.168.151:8880/siltwms/tsunami/ExportServlet?ExportedFilename={file_name}"

        download_response = requests.get(file_url, headers=self.headers)

        if download_response.status_code != 200:
            print(f"Falha ao baixar o arquivo. Status: {download_response.status_code}")
            return None

        def obter_nome(id_relatorio):
            descricao_path = os.path.join(self.google_drive_path, "ProjectsDeA", "ServiceAccountCredentials", "descricao.json")
            if os.path.exists(descricao_path):
                with open(descricao_path, 'r', encoding='utf-8') as f:
                    nomes = json.load(f)
                    for item in nomes:
                        if item.get('idConsultaDinamica') == id_relatorio:
                            return item.get('descricao', 'sem_nome')
                return "relatorio_nao_encontrado"
            else:
                print("Arquivo 'descricao.json' não encontrado.")
                return "erro_arquivo"

        final_name = nome_arquivo if nome_arquivo else obter_nome(id_relatorio)
        full_path = os.path.join(save_path, final_name + ".csv")

        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        with open(full_path, 'wb') as file:
            file.write(download_response.content)

        print(f"Arquivo salvo com sucesso em: {full_path}")
        return full_path

    def renomear_arquivo_baixado(self, caminho=None, nome_original="", sufixo=""):
        if caminho is None:
            caminho = self.save_path
        nome_base, extensao = os.path.splitext(nome_original)
        novo_nome = f"{nome_base}_{sufixo}{extensao}"

        caminho_antigo = os.path.join(caminho, nome_original)
        caminho_novo = os.path.join(caminho, novo_nome)

        if os.path.exists(caminho_antigo):
            if os.path.exists(caminho_novo):
                os.remove(caminho_novo)
            os.rename(caminho_antigo, caminho_novo)
            print(f"Arquivo renomeado para: {novo_nome}")
        else:
            print(f"Arquivo {nome_original} não encontrado para renomear.")

    def gerenciador_coletas(self, nome_usuario, senha_usuario, id_armazem, save_path, nome_arquivo=None, show_filter=[0]):
        try:
            response_json = self.login_wms(nome_usuario, senha_usuario, id_armazem=id_armazem).json()

            bearer_token = response_json.get('value', {}).get('bearer')
            self.headers['Authorization'] = f'Bearer {bearer_token}'
            print("Login realizado com sucesso.")

            grid_id_url = self.link_wms + r'webresources/ColetaService/getGerenciadorColeta'

            grid_id_data = {
                "idArmazem": id_armazem,
                "config": {
                    "@class": "SqlQueryResultCsvConfig",
                    "advancedSearch": [],
                    "customWhere": None,
                    "dynamicParameters": None,
                    "filterConfigs": [],
                    "onlyGenerateSql": False,
                    "orderBy": "IDCOLETA DESC",
                    "parameters": None,
                    "queryType": "TABLEID",
                    "scalarTypes": {
                        "VOLUMESSEPARADOS": "java.lang.Boolean"
                    },
                    "separator": 1,
                    "showAll": True,
                    "showFilter": show_filter,
                    "skip": 0,
                    "sqlQueryLoadMode": "VALUES",
                    "take": 40,
                    "visibleColumnIndex": ""
                }
            }

            grid_id_response = self.session.post(grid_id_url, json=grid_id_data, headers=self.headers)

            if grid_id_response.status_code == 200:
                print("Requisição getDinamicGridID bem-sucedida!")
                grid_id_json = grid_id_response.json()

                value_column_index = grid_id_json.get('config', {}).get('visibleColumnIndex', None)
                if value_column_index:
                    grid_id_data["config"]["visibleColumnIndex"] = value_column_index

                file_path = grid_id_json.get('value', {}).get('filePath')
                file_name = grid_id_json.get('value', {}).get('fileName')

                if file_path and file_name:
                    print(f"Arquivo pronto para ser baixado: {file_name}")

                    self.baixar_csv_wms(file_name, save_path, nome_arquivo, id_relatorio=id_armazem)
                else:
                    print("Caminho ou nome do arquivo não encontrado.")
            else:
                print(f"Falha ao obter Grid ID. Status: {grid_id_response.status_code}")
                print("Resposta:", grid_id_response.text)

        except Exception as e:
            print(f"Erro crítico: {e}")
            raise

        finally:
            self.session.close()
            print("Sessão encerrada com sucesso.")

    def mapa_alocacao(self, nome_usuario, senha_usuario, save_path, nome_arquivo=None, id_relatorio=None, id_armazem=7):
        from datetime import datetime, timedelta

        try:
            response_json = self.login_wms(nome_usuario, senha_usuario, id_armazem=id_armazem).json()

            bearer_token = response_json.get('value', {}).get('bearer')
            self.headers['Authorization'] = f'Bearer {bearer_token}'
            print("Login realizado com sucesso.")

            grid_id_url = self.link_wms + 'webresources/GerenciadorMapaAlocacaoService/getListaMapaAlocacao'

            now = datetime.now()
            start_date = now - timedelta(days=30)
            timestamp_start = int(start_date.timestamp() * 1000)
            timestamp_end = int(now.timestamp() * 1000)

            grid_id_data = {
                "idArmazem": id_armazem,
                "config": {
                    "@class": "SqlQueryResultCsvConfig",
                    "advancedSearch": [],
                    "customWhere": None,
                    "dynamicParameters": None,
                    "filterConfigs": [
                        {
                            "field": "DEPOSITANTE",
                            "comparison": None,
                            "type": "string",
                            "useAnd": False,
                            "map": {"value": "alpa%"}
                        },
                        {
                            "field": "DTGERACAOMAPAALOCACAO",
                            "comparison": "after",
                            "type": "date",
                            "useAnd": True,
                            "map": {"value": timestamp_start}
                        },
                        {
                            "field": "DTGERACAOMAPAALOCACAO",
                            "comparison": "before",
                            "type": "date",
                            "useAnd": True,
                            "map": {"value": timestamp_end}
                        }
                    ],
                    "onlyGenerateSql": False,
                    "orderBy": None,
                    "parameters": None,
                    "queryType": "ROWID",
                    "scalarTypes": {},
                    "separator": 1,
                    "showAll": True,
                    "showFilter": [2],
                    "skip": 0,
                    "sqlQueryLoadMode": "VALUES",
                    "take": 40,
                    "visibleColumnIndex": ""
                }
            }

            grid_id_response = self.session.post(grid_id_url, json=grid_id_data, headers=self.headers)

            if grid_id_response.status_code != 200:
                print(f"Falha ao obter Grid ID. Status: {grid_id_response.status_code}")
                print("Resposta:", grid_id_response.text)
                return

            print("Requisição getDinamicGrid bem-sucedida.")
            grid_id_json = grid_id_response.json()

            value_column_index = grid_id_json.get('config', {}).get('visibleColumnIndex')
            if value_column_index:
                grid_id_data["config"]["visibleColumnIndex"] = value_column_index

            file_path = grid_id_json.get('value', {}).get('filePath')
            file_name = grid_id_json.get('value', {}).get('fileName')

            if not file_path or not file_name:
                print("Caminho ou nome do arquivo não encontrado.")
                return

            print(f"Arquivo pronto para ser baixado: {file_name}")

            self.baixar_csv_wms(file_name, save_path, nome_arquivo, id_relatorio=id_relatorio)

        except Exception as e:
            print(f"Erro crítico: {e}")
            raise

        finally:
            self.session.close()
            print("Sessão encerrada com sucesso.")

    def acompanhamento_nf(self, nome_usuario, senha_usuario, data_inicial, data_final, save_path, nome_arquivo=None, id_armazem=7):
        try:
            response_json = self.login_wms(nome_usuario, senha_usuario, id_armazem=id_armazem).json()

            bearer_token = response_json.get('value', {}).get('bearer')
            self.headers['Authorization'] = f'Bearer {bearer_token}'
            print("Login realizado com sucesso.")

            grid_id_url = r'http://200.143.168.151:8880/siltwms/webresources/GridService/getDinamicGrid'

            grid_id_data = {
                "view": "vt_acompanhamentosaidanf",
                "config": {
                    "@class": "SqlQueryResultCsvConfig",
                    "filterConfigs": [
                        {"field": "H$STATUSNF", "comparison": "in", "type": "enum", "useAnd": False, "map": {"value": "C"}},
                        {"field": "MOVESTOQUE", "comparison": None, "type": "string", "useAnd": False, "map": {"value": "S"}},
                        {"field": "SOLICITACAOCANCELAMENTO", "comparison": None, "type": "string", "useAnd": False, "map": {"value": "0"}},
                        {"field": "DATACADASTROIMPORTACAO", "comparison": "after", "type": "date", "useAnd": True, "map": {"value": data_inicial}},
                        {"field": "DATACADASTROIMPORTACAO", "comparison": "before", "type": "date", "useAnd": True, "map": {"value": data_final}},
                        {"field": "H$IDARMAZEM", "comparison": "eq", "type": "numeric", "useAnd": False, "map": {"value": 7}}
                    ],
                    "onlyGenerateSql": False,
                    "separator": 1,
                    "showAll": True,
                    "skip": 0,
                    "take": 40,
                    "sqlQueryLoadMode": "VALUES",
                    "visibleColumnIndex": "IDNOTAFISCAL,IDPRENF,PEDIDO,NOTAFISCAL,TIPONF,SERIE,CLASSIFICACAOTIPOPEDIDO,CODINTCLASSIFICACAOTPPEDIDO,DATAESPERADAEMBARQUE,EMBARQUEPRIORITARIO,STATUSNF,SEPARACAOINICIADA,SEPARACAOCONCLUIDA,CONFERENCIAINICIADA,CONFERENCIACONCLUIDA,PESADA,PROCESSADO,ROTEIRIZADO,TIPOSAIDA,IDROMANEIO,CODROMANEIO,TITULOROMANEIO,OBSERVACAO,CODIGOSERVICO,DESCSERVICOTRANSP,EMBARQUELIBERADO,CARGA,ESTOQUEVERIFICADO,FATURADO,STATUSRETENCAO,DESCRMOTIVO,USUARIORETENCAO,DATAHORARETENCAO,USUARIOLIBERACAORETENCAO,DATAHORALIBRETENCAO,AUDITADO,IMPRESSOENTREGA,REENTREGA,CADASTRADAEM,IMPORTADOEM,USUARIOIMPORTACAO,VERIFICADOEM,ROTEIZADOEM,SEPARADOEM,USUARIOSEPARACAO,CONFERIDOEM,USUARIOCONFERENCIA,PESADOEM,USUARIOPESAGEM,ENVIADOFATURAMENTO,FATURADOEM,IMPRESSOEM,USUARIOIMPRESSAO,CANCELADOEM,COLETADOEM,USUARIOCOLETA,PROCESSADOEM,QTDEPRODUTO,QTDETOTALPRODUTO,QTDEVOLUMES,PESOVOLUMES,PESOTEORICO,CUBAGEMM3,VLRTOTALNF,CNPJDEPOSITANTE,DEPOSITANTE,CNPJEMITENTE,EMITENTE,CNPJCGCDESTINATARIO,DESTINATARIO,CNPJENTREGA,ENTREGA,CNPJTRANSPORTADORA,TRANSPORTADORA,TRANSPREDESPACHO,MOTORISTA,CFOP,OPERACAO,FRETEPORCONTA,PRECARGA,ROTA,USUARIOALTERACAOROTA,DATAALTERACAOROTA,UTILIZAZPL,CODIGORASTREIO,QTDETAREFAS,QTDEOCORRENCIADOCSAIDA,PONTOALERTA,TIPOUSUARIO,TIPOLIBPA,USUARIOPA,DTCRIACAOPA,USUARIOLIBPA,DTLIBERACAOPA,MOVESTOQUE,IDENTIFICADOR,MOTIVOQUARENTENA,IDMOTIVOQUARENTENA,SOLICITACAOCANCELAMENTO,DATASOLICITCANCELAMENTO,UF_DEST,CLASSIFICACAOCLIENTE,PRIORIDADE,PORCENTAGEMCXFECHADA,FURAFILA,RETIRADACONFIRMADA,USUCONFIRMPEDWEB,DATACONFIRMACAOPEDIDOWEB,DATACADASTROIMPORTACAO,DATAIMPORTACAOPDF,PDFIMPORTADO,SEMANARECEBIMENTO,ANORECEBIMENTO,SEMANADISPONIBILIZACAO,ANODISPONIBILIZACAO,IDPEDIDOPAI"
                }
            }

            grid_id_response = self.session.post(grid_id_url, json=grid_id_data, headers=self.headers)

            if grid_id_response.status_code != 200:
                print(f"Falha ao obter Grid ID. Status: {grid_id_response.status_code}")
                print("Resposta:", grid_id_response.text)
                return

            print("Requisição getDinamicGridID bem-sucedida.")
            grid_id_json = grid_id_response.json()

            value_column_index = grid_id_json.get('config', {}).get('visibleColumnIndex')
            if value_column_index:
                grid_id_data["config"]["visibleColumnIndex"] = value_column_index

            file_path = grid_id_json.get('value', {}).get('filePath')
            file_name = grid_id_json.get('value', {}).get('fileName')

            if not file_path or not file_name:
                print("Caminho ou nome do arquivo não encontrado.")
                return

            print(f"Arquivo pronto para ser baixado: {file_name}")

            self.baixar_csv_wms(file_name, save_path, nome_arquivo, id_relatorio=7)

        except Exception as e:
            print(f"Erro crítico: {e}")
            raise

        finally:
            self.session.close()
            print("Sessão encerrada com sucesso.")
