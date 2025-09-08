import os
import sys
import json
import time
import gspread
import warnings
import datetime
import requests
import numpy as np
import pandas as pd
import datetime as dt
from datetime import date, timedelta
from oauth2client.service_account import ServiceAccountCredentials

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
            descricao_path = os.path.join(self.google_drive_path, "ProjectsDeA", "Service Credentials", "descricao.json")
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

    def gerenciador_coleta(self, nome_usuario, senha_usuario, id_armazem, save_path, nome_arquivo=None, show_filter=[0]):
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

                    self.baixar_csv_wms(file_name, save_path, nome_arquivo, id_relatorio=id_relatorio)
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
                    "visibleColumnIndex": ""
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

            self.baixar_csv_wms(file_name, save_path, nome_arquivo, id_relatorio=id_relatorio)

        except Exception as e:
            print(f"Erro crítico: {e}")
            raise

        finally:
            self.session.close()
            print("Sessão encerrada com sucesso.")

    def gerenciador_volume(self, nome_usuario, senha_usuario, id_armazem, data_inicial, data_final, save_path, id_relatorio, nome_arquivo=None): 
        try:
            response_json = self.login_wms(nome_usuario, senha_usuario, id_armazem=id_armazem).json()
            
            bearer_token = response_json.get('value', {}).get('bearer')
            self.headers['Authorization'] = f'Bearer {bearer_token}'
            print("Login realizado com sucesso.")

            grid_id_url = self.link_wms + r'webresources/GerenciadorVolumeService/getGerenciadorVolume'

            grid_id_data = {
                "idArmazem": id_armazem,
                "config": {
                    "@class": "SqlQueryResultCsvConfig",
                    "sqlQueryLoadMode": "VALUES",
                    "queryType": "ROWID",
                    "showAll": True,
                    "advancedSearch": [],
                    "customWhere": None,
                    "dynamicParameters": None,
                    "filterConfigs": [
                        {
                            "field": "H$STATUSNOTA",
                            "comparison": "in",
                            "type": "enum",
                            "useAnd": False,
                            "map": {"value": "C"}
                        },
                        {
                            "field": "DTGERACAO",
                            "comparison": "after",
                            "type": "date",
                            "useAnd": True,
                            "map": {"value": data_inicial}
                        },
                        {
                            "field": "DTGERACAO",
                            "comparison": "before",
                            "type": "date",
                            "useAnd": True,
                            "map": {"value": data_final}
                        }
                    ],
                    "onlyGenerateSql": False,
                    "orderBy": None,
                    "parameters": None,
                    "scalarTypes": {
                        "H$TIPO": "java.lang.Long",
                        "UTILIZAZPL": "java.lang.Boolean"
                    },
                    "separator": 1,
                    "showFilter": [0],
                    "skip": 0,
                    "take": 40,
                    "visibleColumnIndex": ""
                }
            }

            grid_id_response = self.session.post(grid_id_url, json=grid_id_data, headers=self.headers)

            if grid_id_response.status_code == 200:
                print("Requisição getGerenciadorVolume bem-sucedida!")
                grid_id_json = grid_id_response.json()

                file_name = grid_id_json.get('value', {}).get('fileName')
                file_path = grid_id_json.get('value', {}).get('filePath')

                if file_name and file_path:
                    print(f"Arquivo pronto para ser baixado: {file_name}")

                    self.baixar_csv_wms(file_name, save_path, nome_arquivo, id_relatorio)
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

    def pcp(self, nome_usuario, senha_usuario, id_relatorio, data_inicial, data_final, save_path, nome_arquivo=None):
        try:
            response_json = self.login_wms(nome_usuario, senha_usuario, id_armazem=id_armazem).json()

            bearer_token = response_json.get('value', {}).get('bearer')
            self.headers['Authorization'] = f'Bearer {bearer_token}'
            print("Login realizado com sucesso!")

            grid_url = self.link_wms + r'webresources/GridService/getDinamicGridID'

            usuario_data = {
                "id": 6501,
                "nomeUsuario": "user_1",
                "senha": "passw_1",
                "ativo": True,
                "departamento": "ALPARGATAS",
                "nomeUsuarioCompleto": "ANDERSON PEREIRA SANTOS",
                "tipoUsuario": "SUPERVISOR",
                "utilizaSenhaCaseSensitive": True
            }

            armazem_data = {
                "id": 7,
                "descricao": "LUFT SOLUTIONS - AG2 - CAJAMAR - 16",
                "codigo": "7",
                "ativo": True
            }

            parametros_data = {
                "DEPOSITANTE": " ",
                "DtGeracaoInicial": data_inicial,
                "DtGeracaoFinal": data_final
            }

            print("Enviando requisição para gerar CSV...")

            csv_payload = {
                "id": id_relatorio,
                "armazem": armazem_data,
                "config": {
                    "@class": "SqlQueryResultCsvConfig",
                    "sqlQueryLoadMode": "DEFAULT",
                    "queryType": "ROWID",
                    "showAll": True,
                    "separator": 1,
                    "dynamicParameters": parametros_data,
                    "skip": 0,
                    "take": 100000,
                    "visibleColumnIndex": ""
                },
                "usuario": usuario_data
            }

            grid_response = self.session.post(grid_url, json=csv_payload, headers=self.headers)

            if grid_response.status_code != 200:
                print(f"Erro na requisição CSV: {grid_response.status_code} - {grid_response.text}")
                return

            response_data = grid_response.json()
            file_name = response_data.get('value', {}).get('fileName')

            if not file_name:
                print("Arquivo não gerado ou nome não retornado.")
                return

            print(f"Arquivo gerado: {file_name}")

            self.baixar_csv_wms(file_name, save_path, nome_arquivo, id_relatorio)

        except Exception as e:
            print(f"Erro crítico: {e}")
            raise

        finally:
            self.session.close()
            print("Sessão encerrada com sucesso.")
    
    def produtividade(self, nome_usuario, senha_usuario, id_relatorio, data_inicial, data_final, save_path, nome_arquivo=None):
        try:
            response_json = self.login_wms(nome_usuario, senha_usuario, id_armazem=id_armazem).json()

            bearer_token = response_json.get('value', {}).get('bearer')
            self.headers['Authorization'] = f'Bearer {bearer_token}'
            print("Login realizado com sucesso!")

            grid_id_url = self.link_wms + r'webresources/GridService/getDinamicGridID'

            grid_id_data = {
                "id": id_relatorio,
                "config": {
                    "@class": "SqlQueryResultCsvConfig",
                    "sqlQueryLoadMode": "DEFAULT",
                    "queryType": "ROWID",
                    "showAll": True,
                    "dynamicParameters": {
                        "Data_Inicial": data_inicial,
                        "Data_Final": data_final,
                        "DataInicio": data_inicial,
                        "DataFim": data_final
                    },
                    "parameters": {
                        "Data_Inicial": data_inicial,
                        "Data_Final": data_final,
                        "DataInicio": data_inicial,
                        "DataFim": data_final
                    },
                    "separator": 1,
                    "take": 100000,
                    "skip": 0,
                    "visibleColumnIndex": ""
                },
                "armazem": {
                    "id": 7,
                    "descricao": "LUFT SOLUTIONS - AG2 - CAJAMAR - 16"
                },
                "usuario": {
                    "id": 6501,
                    "nomeUsuario": "ANDERSON.SANTOS1"
                }
            }

            grid_id_response = self.session.post(grid_id_url, json=grid_id_data, headers=self.headers)

            if grid_id_response.status_code != 200:
                print(f"Falha ao obter Grid ID. Status: {grid_id_response.status_code}")
                print("Resposta:", grid_id_response.text)
                return

            grid_id_json = grid_id_response.json()
            print("Requisição getDinamicGridID bem-sucedida!")
            print("Resposta do Grid ID:", grid_id_json)

            file_name = grid_id_json.get('value', {}).get('fileName')
            file_path = grid_id_json.get('value', {}).get('filePath')

            if not file_name or not file_path:
                print("Caminho ou nome do arquivo não encontrado.")
                return

            print(f"Arquivo pronto para ser baixado: {file_name}")

            self.baixar_csv_wms(file_name, save_path, nome_arquivo, id_relatorio)

        except Exception as e:
            print(f"Erro crítico: {e}")
            raise

        finally:
            self.session.close()
            print("Sessão encerrada com sucesso.")

    def relatorio(self, nome_usuario, senha_usuario, id_relatorio, data_inicial, data_final, save_path, nome_arquivo=None):
        try:
            response_json = self.login_wms(nome_usuario, senha_usuario, id_armazem=id_armazem).json()

            bearer_token = response_json.get('value', {}).get('bearer')
            self.headers['Authorization'] = f'Bearer {bearer_token}'
            print("Login Realizado com Sucesso")

            grid_id_url = self.link_wms + r'webresources/GridService/getDinamicGridID'

            grid_id_data = {
                "id": id_relatorio,
                "armazem": {
                    "id": 7,
                    "descricao": "LUFT SOLUTIONS - AG2 - CAJAMAR - 16",
                    "codigo": "7",
                    "ativo": True
                },
                "config": {
                    "@class": "SqlQueryResultCsvConfig",
                    "sqlQueryLoadMode": "DEFAULT",
                    "queryType": "ROWID",
                    "showAll": True,
                    "orderBy": None,
                    "customWhere": None,
                    "scalarTypes": {
                        "BUFFER": "java.lang.Boolean",
                        "LOCALATIVO": "java.lang.Boolean"
                    },
                    "showFilter": [],
                    "filterConfigs": [],
                    "take": 50,
                    "skip": 0,
                    "advancedSearch": [],
                    "parameters": {
                        "Data_Inicial": data_inicial,
                        "Data_Final": data_final,
                        "Data_Inicio": data_inicial,
                        "Data_Fim": data_final,
                        "DataInicial": data_inicial,
                        "DataFinal": data_final,
                        "DataInicio": data_inicial,
                        "DataFim": data_final
                    },
                    "onlyGenerateSql": False,
                    "visibleColumnIndex": "",
                    "separator": 1
                },
                "usuario": {
                    "id": self.id_token_wms,
                    "nomeUsuario": LOGINS_WMS[0],
                    "senha": self.token_senha_wms,
                    "ativo": True
                }
            }

            grid_id_response = self.session.post(grid_id_url, json=grid_id_data, headers=self.headers)

            if grid_id_response.status_code != 200:
                print(f"Falha ao obter Grid ID. Status: {grid_id_response.status_code}")
                print("Resposta:", grid_id_response.text)
                return

            grid_id_json = grid_id_response.json()
            print("Requisição getDinamicGridID bem-sucedida!")
            print("Resposta do Grid ID:", grid_id_json)

            file_name = grid_id_json.get('value', {}).get('fileName')
            file_path = grid_id_json.get('value', {}).get('filePath')

            if not file_name or not file_path:
                print("Caminho ou nome do arquivo não encontrado.")
                return

            print(f"Arquivo pronto para ser baixado: {file_name}")

            self.baixar_csv_wms(file_name, save_path, nome_arquivo, id_relatorio)

        except Exception as e:
            print(f"Erro crítico: {e}")
            raise

        finally:
            self.session.close()
            print("Sessão encerrada com sucesso.")

    def estoque(self, nome_usuario, senha_usuario, id_depositante, save_path):
        try:
            response_json = self.login_wms(nome_usuario, senha_usuario, id_armazem=id_armazem).json()

            bearer_token = response_json.get('value', {}).get('bearer')
            self.headers['Authorization'] = f'Bearer {bearer_token}'
            print("Login Realizado com Sucesso")

            consulta_estoque_url = self.link_wms + r'webresources/ConsultaEstoqueService/getConsultaEstoqueLocalPorProduto'

            gerar_csv_data = {
                "idDepositante": id_depositante,
                "idArmazem": 7,
                "config": {
                    "@class": "SqlQueryResultCsvConfig",
                    "sqlQueryLoadMode": "VALUES",
                    "queryType": "TABLEID",
                    "showAll": True,
                    "orderBy": None,
                    "customWhere": None,
                    "scalarTypes": {
                        "BUFFER": "java.lang.Boolean",
                        "LOCALATIVO": "java.lang.Boolean"
                    },
                    "separator": 1,
                    "showFilter": [],
                    "filterConfigs": [],
                    "take": 40,
                    "skip": 0,
                    "advancedSearch": [],
                    "parameters": None,
                    "onlyGenerateSql": False,
                    "dynamicParameters": None,
                    "visibleColumnIndex": ""
                }
            }

            print("Gerando CSV...")
            gerar_csv_response = self.session.post(consulta_estoque_url, json=gerar_csv_data, headers=self.headers)

            if gerar_csv_response.status_code != 200:
                print(f"Falha ao gerar o CSV! Status: {gerar_csv_response.status_code}")
                return

            print("CSV gerado com sucesso!")
            gerar_csv_response_json = gerar_csv_response.json()

            value_column_index = gerar_csv_response_json.get('config', {}).get('visibleColumnIndex', None)
            if value_column_index is None:
                print("visibleColumnIndex não encontrado na resposta!")

            if value_column_index is not None:
                gerar_csv_data["config"]["visibleColumnIndex"] = value_column_index

            file_name = gerar_csv_response_json['value'].get('fileName')
            if not file_name:
                print("Nome do arquivo CSV não encontrado.")
                return

            print(f"Baixando o arquivo CSV: {file_name}...")
            self.baixar_csv_wms(file_name, save_path, nome_arquivo="Estoque Local Por Produto")

        except Exception as e:
            print(f"Erro crítico: {e}")
            raise

        finally:
            self.session.close()
            print("Sessão encerrada com sucesso.")

    def deposito(self, nome_usuario, senha_usuario, id, id_deposito, data_inicial, data_final, save_path):
        try:
            response_json = self.login_wms(nome_usuario, senha_usuario, id_armazem=id_armazem).json()

            bearer_token = response_json.get('value', {}).get('bearer')
            self.headers['Authorization'] = f'Bearer {bearer_token}'
            print("Login Realizado com Sucesso")

            grid_id_url = self.link_wms + r'webresources/GridService/getDinamicGridID'

            grid_id_data = {
                "id": id,
                "armazem": {
                    "id": 7,
                    "descricao": "LUFT SOLUTIONS - AG2 - CAJAMAR - 16",
                    "codigo": "7",
                    "ativo": True
                },
                "config": {
                    "@class": "SqlQueryResultCsvConfig",
                    "sqlQueryLoadMode": "DEFAULT",
                    "queryType": "ROWID",
                    "showAll": True,
                    "orderBy": None,
                    "customWhere": None,
                    "scalarTypes": {
                        "BUFFER": "java.lang.Boolean",
                        "LOCALATIVO": "java.lang.Boolean"
                    },
                    "showFilter": [],
                    "filterConfigs": [],
                    "take": 50,
                    "skip": 0,
                    "advancedSearch": [],
                    "parameters": {
                        "identidade_dep": id_deposito,
                        "identidade": id_deposito,
                        "Data_Inicial": data_inicial,
                        "Data_Final": data_final,
                        "Data_Inicio": data_inicial,
                        "Data_Fim": data_final,
                        "DataInicial": data_inicial,
                        "DataFinal": data_final,
                        "DataInicio": data_inicial,
                        "DataFim": data_final,
                        "data_inicio": data_inicial,
                        "data_fim": data_final
                    },
                    "onlyGenerateSql": False,
                    "visibleColumnIndex": "",
                    "separator": 1
                },
                "usuario": {
                    "id": self.id_token_wms,
                    "nomeUsuario": nome_usuario,
                    "senha": self.token_senha_wms,
                    "ativo": True
                }
            }

            grid_id_response = self.session.post(grid_id_url, json=grid_id_data, headers=self.headers)

            if grid_id_response.status_code != 200:
                print(f"Falha ao obter Grid ID. Status: {grid_id_response.status_code}")
                print("Resposta:", grid_id_response.text)
                return

            grid_id_json = grid_id_response.json()
            print("Requisição getDinamicGridID bem-sucedida!")
            print("Resposta do Grid ID:", grid_id_json)

            file_name = grid_id_json.get('value', {}).get('fileName')
            file_path = grid_id_json.get('value', {}).get('filePath')

            if not file_name or not file_path:
                print("Caminho ou nome do arquivo não encontrado.")
                return

            print(f"Arquivo pronto para ser baixado: {file_name}")

            self.baixar_csv_wms(file_name, save_path, nome_arquivo, id_relatorio)

        except Exception as e:
            print(f"Erro crítico: {e}")
            raise

        finally:
            self.session.close()
            print("Sessão encerrada com sucesso.")

    def execute_notebook(self, notebook_path):
        try:
            with open(notebook_path, 'r', encoding='utf-8') as f:
                notebook_content = nbformat.read(f, as_version=4)

            client = NotebookClient(notebook_content)
            client.execute()

            print(f"Notebook {notebook_path} executado com sucesso!")

        except Exception as e:
            print(f"Erro ao executar o notebook {notebook_path}: {e}")

    def upload_sheet(self, spreadsheet_data, force_text=False):
        def convert_datetime_to_str(x):
            if isinstance(x, (pd.Timestamp, datetime.datetime, np.datetime64)):
                return pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S')
            return x

        self.scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        self.creds_path = os.path.join(
            self.get_google_drive_path(), 'ProjectsDeA', 'Service Credentials', 'token.json'
        )

        creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_path, self.scope)
        self.client = gspread.authorize(creds)

        value_input_mode = 'RAW' if force_text else 'USER_ENTERED'

        for spreadsheet_info in spreadsheet_data:
            sheet = self.client.open_by_key(spreadsheet_info['spreadsheet_id'])

            for aba_gs, arquivo_path in spreadsheet_info['abas_e_csv'].items():
                ext = os.path.splitext(arquivo_path)[1].lower()

                try:
                    if ext == '.csv':
                        df_all = {"DEFAULT": pd.read_csv(arquivo_path)}
                    else:
                        excel_file = pd.ExcelFile(arquivo_path)
                        if aba_gs == 'ALL_SHEETS':
                            df_all = {aba: pd.read_excel(arquivo_path, sheet_name=aba) for aba in excel_file.sheet_names}
                        else:
                            df_all = {aba_gs: pd.read_excel(arquivo_path, sheet_name=excel_file.sheet_names[0])}
                except Exception as e:
                    print(f"Erro ao ler o arquivo {arquivo_path}: {e}")
                    continue

                for aba_nome, df in df_all.items():
                    df = df.fillna('')
                    df = df.applymap(convert_datetime_to_str)

                    try:
                        worksheet = sheet.worksheet(aba_nome)
                    except gspread.exceptions.WorksheetNotFound:
                        worksheet = sheet.add_worksheet(title=aba_nome, rows=10000, cols=100)
                        time.sleep(1)

                    worksheet.clear()

                    try:
                        worksheet.update(
                            [df.columns.values.tolist()] + df.values.tolist(),
                            value_input_option=value_input_mode
                        )
                    except Exception as e:
                        print(f"Erro ao atualizar a aba '{aba_nome}': {e}")

        print("Planilhas atualizadas com sucesso!")
