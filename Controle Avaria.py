import requests
import json
import time
import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys, os
import warnings
import importlib
from datetime import date, timedelta


warnings.filterwarnings("ignore")

sys.path.append(
    r"G:\Meu Drive\ProjectsDeA" if os.path.exists(r"G:\Meu Drive") 
    else r"H:\Meu Drive\ProjectsDeA" if os.path.exists(r"H:\Meu Drive") 
    else r"/home/luftsolutions/GoogleDrive/ProjectsDeA"
)

from config import *
EstruturaModule = importlib.import_module("Estrutura")
Estrutura = EstruturaModule.Estrutura

google_drive_path = Estrutura().get_google_drive_path()

class AvariaManager:
    def __init__(self):
        self.BASE_URL = 'http://200.143.168.151:8880/siltwms/webresources'
        self.ENDPOINTS = {
            'login': '/SessionService/login',
            'autorizacao_supervisor': '/SessionService/autorizacaoSupervisor',
            'consulta_avaria': '/ConsultaControleAvariaService/getConsultaControleAvaria',
            'lote_avaria': '/ConsultaControleAvariaService/getLoteControleAvaria',
            'add_lotes': '/ConsultaControleAvariaService/addLotes',
            'finalizar_avaria': '/ConsultaControleAvariaService/finalizarAvaria',
            'get_remanejamento': '/RemanejamentoService/getRemanejamento',
            'finalizar_remanejamento': '/RemanejamentoService/finalizarPlanejamentoWeb',
            'criar_avaria': '/ControleAvariaCRUD/save',
            'locais_origem': '/ConsultaControleAvariaService/getLocaisOrigemAvaria',
            'locais_destino': '/ConsultaControleAvariaService/getLocaisDestinoAvaria',
            'motivos': '/MotivoOcorrenciaService/getListaMotivosOcorrencias',
            'server_time': '/ServerTimeService/getCurrentTime'
        }
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Http-Method-Override': 'POST'
        }
        
        self.session = self._create_session()
        self.id_armazem = 7
        self.usuario = LOGINS_WMS[0]
        self.senha = SENHAS_WMS[0]
        self.token_senha = TOKENS_SENHAS[0]
        self.id_token = ID_TOKEN_WMS[0]
        
    def _create_session(self):
        """Cria uma sessão HTTP com política de retry"""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[408, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
    
    def _get_server_time(self):
        """Obtém a data/hora atual do servidor para sincronização"""
        try:
            response = self.session.get(
                self.BASE_URL + self.ENDPOINTS['server_time'],
                headers=self.headers,
                timeout=5
            )
            if response.status_code == 200:
                server_time = response.json().get('value', {}).get('currentTime')
                if server_time:
                    return datetime.datetime.fromtimestamp(server_time/1000)
        except Exception as e:
            print(f"Não foi possível obter hora do servidor: {str(e)}")
        return datetime.datetime.now()
    
    def login(self):
        """Realiza o login no sistema e obtém o token de autenticação"""
        login_data = {
            "nomeUsuario": self.usuario,
            "password": self.senha,
            "armazem": {
                "id": self.id_armazem,
                "descricao": "LUFT SOLUTIONS - AG2 - CAJAMAR - 16"
            }
        }
        
        print("Efetuando login...")
        try:
            response = self.session.post(
                self.BASE_URL + self.ENDPOINTS['login'],
                json=login_data,
                headers=self.headers
            )
            response.raise_for_status()
            
            bearer_token = response.json().get('value', {}).get('bearer')
            if not bearer_token:
                raise Exception("Token de autenticação não encontrado!")
            
            self.headers['Authorization'] = f'Bearer {bearer_token}'
            print("Login realizado com sucesso!")
            
            auth_data = {
                "usuario": self.usuario.lower(),
                "senha": self.senha
            }
            auth_response = self.session.post(
                self.BASE_URL + self.ENDPOINTS['autorizacao_supervisor'],
                json=auth_data,
                headers=self.headers
            )
            auth_response.raise_for_status()
            print("Autorização de supervisor concedida")
            return True
            
        except Exception as e:
            raise Exception(f"Falha no login: {str(e)}")
    
    def criar_avaria(self):
        """Cria uma nova avaria no sistema com data sincronizada"""
        server_time = self._get_server_time()
        current_time = int(server_time.timestamp() * 1000)
        print(f"Criando avaria com data do servidor: {server_time.strftime('%d/%m/%Y %H:%M:%S')}")
        
        data = {
            "entity": {
                "id": 0,
                "armazemOrigem": {
                    "id": self.id_armazem,
                    "descricao": "LUFT SOLUTIONS - AG2 - CAJAMAR - 16",
                    "codigo": "7",
                    "ativo": True
                },
                "armazemDestino": {
                    "id": self.id_armazem,
                    "descricao": "LUFT SOLUTIONS - AG2 - CAJAMAR - 16",
                    "codigo": "7",
                    "ativo": True
                },
                "dataAvaria": current_time,
                "dataProcessamento": current_time,
                "estado": "DANIFICADO",
                "finalizado": "N",
                "idLocalDestino": "IV888088888",
                "idLocalOrigem": "IL999999909",
                "listaControleAvariaLote": [],
                "motivo": "PROCESSO AUTOMATICO",
                "motivoOcorrencia": {
                    "id": 56,
                    "codigo": None,
                    "descricao": None,
                    "ativo": False,
                    "utilizadoControleAvaria": False
                },
                "usuario": {
                    "id": self.id_token,
                    "nomeUsuario": None,
                    "senha": None,
                    "ativo": False,
                    "tipoUsuario": None,
                    "codBarra": None
                }
            },
            "id": None,
            "tela": "Cadastro de Controle de Avaria",
            "usuario": {
                "id": self.id_token,
                "nomeUsuario": self.usuario,
                "senha": self.token_senha,
                "ativo": True
            }
        }
        
        print("Criando nova avaria...")
        try:
            response = self.session.post(
                self.BASE_URL + self.ENDPOINTS['criar_avaria'],
                json=data,
                headers=self.headers
            )
            response.raise_for_status()
            
            result = response.json()
            id_avaria = result.get('id')
            if not id_avaria and 'value' in result:
                if isinstance(result['value'], dict):
                    id_avaria = result['value'].get('id')
                elif isinstance(result['value'], str):
                    try:
                        value_data = json.loads(result['value'])
                        id_avaria = value_data.get('id')
                    except json.JSONDecodeError:
                        pass
            
            if not id_avaria and 'armazemOrigem' in result and 'id' in result:
                id_avaria = result['id']
            
            if not id_avaria:
                raise Exception("ID da avaria não retornado na resposta!")
            
            print(f"Avaria criada com sucesso! ID: {id_avaria}")
            return id_avaria
            
        except Exception as e:
            print("Dados enviados para criação de avaria:", json.dumps(data, indent=2))
            raise Exception(f"Erro ao criar avaria: {str(e)}")
            
    def obter_controle_avaria(self, id_avaria=None):
        """Obtém dados da avaria"""
        try:
            if id_avaria:
                try:
                    response = self.session.get(
                        f"{self.BASE_URL}/ConsultaControleAvariaService/getAvaria/{id_avaria}",
                        headers=self.headers,
                        timeout=10
                    )
                    if response.status_code == 200:
                        return response.json().get('value', {})
                except Exception:
                    pass

            hoje = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            timestamp_hoje = int(hoje.timestamp() * 1000)
            
            payload = {
                "idArmazem": self.id_armazem,
                "config": {
                    "@class": "SqlQueryResultSlickConfig",
                    "sqlQueryLoadMode": "VALUES",
                    "queryType": "ROWID",
                    "showAll": False,
                    "filterConfigs": [
                        {
                            "field": "DTAVARIA",
                            "comparison": "on",
                            "type": "date",
                            "useAnd": False,
                            "map": {"value": timestamp_hoje}
                        }
                    ],
                    "skip": 0,
                    "take": 40
                }
            }

            if id_avaria:
                payload["config"]["filterConfigs"].append({
                    "field": "IDCONTROLEAVARIA",
                    "comparison": "eq",
                    "type": "numeric",
                    "useAnd": True,
                    "map": {"value": int(id_avaria)}
                })

            response = self.session.post(
                self.BASE_URL + self.ENDPOINTS['consulta_avaria'],
                json=payload,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()

            response_data = response.json()
            result_data = []
            if 'value' in response_data:
                if isinstance(response_data['value'], dict) and 'value' in response_data['value']:
                    try:
                        parsed = json.loads(response_data['value']['value'])
                        result_data = parsed.get("resultData", [])
                    except:
                        result_data = []
                elif isinstance(response_data['value'], str):
                    try:
                        parsed = json.loads(response_data['value'])
                        result_data = parsed.get("resultData", [])
                    except:
                        result_data = []

            if not result_data:
                raise Exception("Nenhum dado retornado na consulta")

            if id_avaria:
                return result_data[0] if result_data else None
            return result_data

        except Exception as e:
            raise Exception(f"Falha ao consultar avaria: {str(e)}")

    def obter_lotes_avaria(self, id_avaria):
        """Obtém os lotes disponíveis para a avaria"""
        payload = {
            "idArmazem": self.id_armazem,
            "idLocalOrigem": "IL999999909",
            "idControleAvaria": id_avaria,
            "config": {
                "@class": "SqlQueryResultTableConfig",
                "sqlQueryLoadMode": "DEFAULT",
                "queryType": "ROWID",
                "showAll": False,
                "skip": 0,
                "take": 1000
            }
        }
        
        print("Consultando lotes para avaria...")
        try:
            response = self.session.post(
                self.BASE_URL + self.ENDPOINTS['lote_avaria'],
                json=payload,
                headers=self.headers
            )
            response.raise_for_status()
            
            lotes_processados = []
            response_data = response.json()
            
            for lote in response_data.get("value", {}).get("lines", []):
                try:
                    columns = lote["columns"]
                    lote_id = columns[1]
                    estoque = columns[3]   
                    
                    if isinstance(estoque, (int, float)) and estoque > 0:
                        lotes_processados.append((lote_id, int(estoque)))
                        print(f"Lote {lote_id} - Estoque: {estoque}")
                    else:
                        print(f"Lote {lote_id} ignorado - Estoque inválido: {estoque}")
                        
                except Exception as e:
                    print(f"Erro ao processar lote: {str(e)}")
                    continue
            
            if not lotes_processados:
                raise Exception("Nenhum lote válido encontrado!")
            
            print(f"Total de lotes válidos: {len(lotes_processados)}")
            return lotes_processados
            
        except Exception as e:
            raise Exception(f"Erro ao obter lotes: {str(e)}")
    
    def adicionar_lotes_avaria(self, id_avaria, lotes):
        """Adiciona lotes à avaria criada"""
        CHUNK_SIZE = 30
        chunks = [lotes[i:i + CHUNK_SIZE] for i in range(0, len(lotes), CHUNK_SIZE)]
        total_processados = 0
        
        print(f"Iniciando processamento em {len(chunks)} lotes de {CHUNK_SIZE}...")
        
        for i, chunk in enumerate(chunks, 1):
            chunk_dict = {str(lote_id): quantidade for lote_id, quantidade in chunk}

            payload = {
                "idUsuario": self.id_token,
                "idControleAvaria": int(id_avaria),
                "idArmazem": self.id_armazem,
                "armazem": {
                    "id": self.id_armazem,
                    "descricao": "LUFT SOLUTIONS - AG2 - CAJAMAR - 16"
                },
                "adicionarLotes": chunk_dict
            }
            
            print(f"Processando lote {i}/{len(chunks)} - {len(chunk)} itens...")
            
            try:
                start_time = time.time()
                response = self.session.post(
                    self.BASE_URL + self.ENDPOINTS['add_lotes'],
                    json=payload,
                    headers=self.headers
                )
                elapsed_time = time.time() - start_time
                
                if response.status_code == 204:
                    print(f"Lote {i} processado com sucesso em {elapsed_time:.2f}s")
                    total_processados += len(chunk)
                else:
                    print(f"Erro no lote {i} (Status {response.status_code})")
                    print(f"Mensagem: {response.text}")
                    
            except Exception as e:
                print(f"Erro grave ao processar lote {i}: {str(e)}")
                continue
        
        return total_processados
    
    def _ajustar_data_avaria(self, id_avaria, nova_data):
        """Ajusta a data da avaria de forma síncrona"""
        print(f"Atualizando data da avaria {id_avaria}")
        
        avaria_data = self.obter_controle_avaria(id_avaria)
        if not avaria_data:
            raise Exception("Não foi possível obter dados da avaria para ajuste")
        
        update_data = {
            "entity": {
                "id": id_avaria,
                "armazemOrigem": avaria_data.get('ARMAZEMORIGEM'),
                "armazemDestino": avaria_data.get('ARMAZEMDESTINO'),
                "dataAvaria": nova_data,
                "dataProcessamento": nova_data,
                "estado": avaria_data.get('ESTADO'),
                "finalizado": avaria_data.get('FINALIZADO'),
                "idLocalDestino": avaria_data.get('IDLOCALDESTINO'),
                "idLocalOrigem": avaria_data.get('IDLOCALORIGEM'),
                "motivo": avaria_data.get('MOTIVO'),
                "motivoOcorrencia": avaria_data.get('MOTIVOOCORRENCIA'),
                "usuario": avaria_data.get('USUARIO')
            },
            "id": id_avaria,
            "tela": "Cadastro de Controle de Avaria",
            "usuario": {
                "id": self.id_token,
                "nomeUsuario": self.usuario,
                "senha": self.token_senha,
                "ativo": True
            }
        }
        
        try:
            response = self.session.put(
                f"{self.BASE_URL}/ControleAvariaCRUD/update",
                json=update_data,
                headers=self.headers
            )
            response.raise_for_status()
            print("Data da avaria atualizada com sucesso")
            time.sleep(1)
        except Exception as e:
            raise Exception(f"Falha ao ajustar data: {str(e)}")
    
    def finalizar_avaria(self, id_avaria):
        """Método robusto para finalização com tratamento completo de erros"""
        max_tentativas = 3
        tentativa = 1
        
        while tentativa <= max_tentativas:
            try:
                print(f"Tentativa {tentativa}/{max_tentativas} de finalização")
                
                server_time = self._get_server_time()
                current_time = int(server_time.timestamp() * 1000)
                print(f"Data/hora do servidor: {server_time.strftime('%d/%m/%Y %H:%M:%S')}")

                avaria_data = self.obter_controle_avaria(id_avaria)
                if not avaria_data:
                    raise Exception("Não foi possível obter dados da avaria")

                data_avaria = avaria_data.get('DTAVARIA')
                if data_avaria and data_avaria > current_time:
                    print(f"Ajustando data (de {data_avaria} para {current_time})")
                    self._ajustar_data_avaria(id_avaria, current_time)
                    time.sleep(1)

                finalizar_data = {
                    "idControleAvaria": int(id_avaria),
                    "idArmazem": self.id_armazem,
                    "usuario": {
                        "id": self.id_token,
                        "nomeUsuario": self.usuario,
                        "senha": self.token_senha,
                        "ativo": True,
                        "codBarra": "0006652",
                        "departamento": "ALPARGATAS",
                        "nomeUsuarioCompleto": "ANDERSON PEREIRA SANTOS",
                        "tipoUsuario": "SUPERVISOR",
                        "utilizaSenhaCaseSensitive": True
                    }
                }

                print("Enviando requisição para finalizar...")
                response = self.session.post(
                    self.BASE_URL + self.ENDPOINTS['finalizar_avaria'],
                    json=finalizar_data,
                    headers=self.headers,
                    timeout=45
                )

                if response.status_code == 204:
                    print("Avaria finalizada com sucesso!")
                    return True
                
                if response.status_code == 500 and "DATA DA AVARIA" in response.text:
                    print("Erro de data detectado, ajustando novamente...")
                    nova_data = int((server_time - datetime.timedelta(minutes=1)).timestamp() * 1000)
                    self._ajustar_data_avaria(id_avaria, nova_data)
                    tentativa += 1
                    continue

                raise Exception(f"Resposta inesperada: {response.status_code} - {response.text}")

            except requests.exceptions.Timeout:
                print("Timeout, tentando novamente...")
                tentativa += 1
                time.sleep(2)
                continue
                
            except Exception as e:
                if tentativa == max_tentativas:
                    raise Exception(f"Falha após {max_tentativas} tentativas: {str(e)}")
                print(f"Erro: {str(e)} - Tentando novamente...")
                tentativa += 1
                time.sleep(1)
                continue

        raise Exception(f"Não foi possível finalizar após {max_tentativas} tentativas")
    
    def verificar_remanejamentos(self):
        """Verifica se existem remanejamentos pendentes para o usuário (usando filtro 4)"""
        payload = {
            "idArmazem": self.id_armazem,
            "config": {
                "@class": 'SqlQueryResultSlickConfig',
                "sqlQueryLoadMode": "VALUES",
                "queryType": "ROWID",
                "showAll": False,
                "filterConfigs": [
                    {
                        "field": "USUARIOCADASTRO",
                        "comparison": None,
                        "type": "string",
                        "useAnd": False,
                        "map": {"value": self.usuario}
                    }
                ],
                "orderBy": "IDREMANEJAMENTO DESC",
                "skip": 0,
                "take": 40,
                "showFilter": [4]  
            }
        }

        print("Verificando remanejamentos pendentes...")
        try:
            response = self.session.post(
                self.BASE_URL + self.ENDPOINTS['get_remanejamento'],
                json=payload,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()

            remanejamentos = []
            response_data = response.json()

            if 'value' in response_data:
                if isinstance(response_data['value'], dict):
                    if 'resultData' in response_data['value']:
                        remanejamentos = response_data['value']['resultData']
                    elif 'value' in response_data['value'] and isinstance(response_data['value']['value'], str):
                        try:
                            parsed = json.loads(response_data['value']['value'])
                            remanejamentos = parsed.get("resultData", [])
                        except json.JSONDecodeError:
                            remanejamentos = []
                elif isinstance(response_data['value'], str):
                    try:
                        parsed = json.loads(response_data['value'])
                        remanejamentos = parsed.get("resultData", [])
                    except json.JSONDecodeError:
                        remanejamentos = []

            if not remanejamentos:
                print("Nenhum remanejamento pendente encontrado")
                return []

            print(f"Total de remanejamentos pendentes encontrados: {len(remanejamentos)}")
            return remanejamentos

        except Exception as e:
            print(f"Erro ao verificar remanejamentos: {str(e)}")
            return []

    def verificar_status_remanejamento(self, id_remanejamento):
        """Verifica se o remanejamento está pendente (usando filtro 4)"""
        payload = {
            "idArmazem": self.id_armazem,
            "config": {
                "@class": 'SqlQueryResultSlickConfig',
                "sqlQueryLoadMode": "VALUES",
                "queryType": "ROWID",
                "showAll": False,
                "filterConfigs": [
                    {
                        "field": "IDREMANEJAMENTO",
                        "comparison": "eq",
                        "type": "numeric",
                        "useAnd": False,
                        "map": {"value": int(id_remanejamento)}
                    }
                ],
                "showFilter": [4],   
                "skip": 0,
                "take": 1
            }
        }

        try:
            response = self.session.post(
                self.BASE_URL + self.ENDPOINTS['get_remanejamento'],
                json=payload,
                headers=self.headers,
                timeout=15
            )
            response.raise_for_status()
            response_data = response.json()

            result_data = []
            if 'value' in response_data:
                if isinstance(response_data['value'], dict) and 'resultData' in response_data['value']:
                    result_data = response_data['value']['resultData']
                elif isinstance(response_data['value'], str):
                    try:
                        parsed = json.loads(response_data['value'])
                        result_data = parsed.get("resultData", [])
                    except json.JSONDecodeError:
                        result_data = []

            return len(result_data) > 0

        except Exception as e:
            print(f"Erro ao verificar pendente do remanejamento {id_remanejamento}: {str(e)}")
            return False


    def finalizar_remanejamentos(self, ids_remanejamentos):
        """Versão melhorada para lidar com remanejamentos já finalizados"""
        if not ids_remanejamentos:
            print("Nenhum ID de remanejamento fornecido")
            return False

        remanejamentos_validos = []
        remanejamentos_ja_finalizados = 0

        for id_reman in ids_remanejamentos:
            if not self.verificar_status_remanejamento(id_reman):
                remanejamentos_validos.append(id_reman)
            else:
                remanejamentos_ja_finalizados += 1
                print(f"Remanejamento {id_reman} já está finalizado")

        print(f"Status dos remanejamentos:")
        print(f" - Total encontrados: {len(ids_remanejamentos)}")
        print(f" - Já finalizados: {remanejamentos_ja_finalizados}")
        print(f" - Pendentes: {len(remanejamentos_validos)}")

        if not remanejamentos_validos:
            print("Nenhum remanejamento pendente para finalizar")
            return True

        BATCH_SIZE = 3
        batches = [remanejamentos_validos[i:i + BATCH_SIZE] for i in range(0, len(remanejamentos_validos), BATCH_SIZE)]
        total_sucesso = 0

        for batch in batches:
            if len(batch) == 1:
                print(f"Tentando finalizar remanejamento: {batch[0]}")
            else:
                print(f"Tentando finalizar lote: {batch}")

            payload = {
                "idsRemanejamentos": batch,
                "idUsuario": self.id_token
            }

            try:
                response = self.session.post(
                    self.BASE_URL + self.ENDPOINTS['finalizar_remanejamento'],
                    json=payload,
                    headers=self.headers,
                    timeout=30
                )

                if response.status_code == 204:
                    if len(batch) == 1:
                        print(f"Remanejamento {batch[0]} finalizado com sucesso!")
                    else:
                        print(f"Lote {batch} finalizado com sucesso!")
                    total_sucesso += len(batch)
                    continue

                print(f"Falha no lote, tentando individualmente...")
                for id_reman in batch:
                    try:
                        individual_payload = {
                            "idsRemanejamentos": [id_reman],
                            "idUsuario": self.id_token
                        }
                        individual_response = self.session.post(
                            self.BASE_URL + self.ENDPOINTS['finalizar_remanejamento'],
                            json=individual_payload,
                            headers=self.headers,
                            timeout=20
                        )

                        if individual_response.status_code == 204:
                            print(f"Remanejamento {id_reman} finalizado")
                            total_sucesso += 1
                        else:
                            print(f"Falha no remanejamento {id_reman}: {individual_response.status_code}")
                            if individual_response.status_code == 500:
                                print(f"Mensagem de erro: {individual_response.text[:200]}...")
                    except Exception as e:
                        print(f"Erro ao processar remanejamento {id_reman}: {str(e)}")

            except Exception as e:
                print(f"Erro ao processar lote {batch}: {str(e)}")
                for id_reman in batch:
                    try:
                        individual_payload = {
                            "idsRemanejamentos": [id_reman],
                            "idUsuario": self.id_token
                        }
                        individual_response = self.session.post(
                            self.BASE_URL + self.ENDPOINTS['finalizar_remanejamento'],
                            json=individual_payload,
                            headers=self.headers,
                            timeout=20
                        )

                        if individual_response.status_code == 204:
                            print(f"Remanejamento {id_reman} finalizado")
                            total_sucesso += 1
                        else:
                            print(f"Falha no remanejamento {id_reman}: {individual_response.status_code}")
                    except Exception as e:
                        print(f"Erro ao processar remanejamento {id_reman}: {str(e)}")

        print(f"Resultado final:")
        print(f" - Total de remanejamentos processados com sucesso: {total_sucesso}")
        print(f" - Total de remanejamentos que falharam: {len(remanejamentos_validos) - total_sucesso}")

        return total_sucesso > 0

    def esperar_e_processar_remanejamentos(self, timeout_minutos=10):
        """Espera o surgimento de remanejamentos pendentes e os finaliza assim que aparecerem"""
        tempo_inicio = time.time()
        tempo_limite = tempo_inicio + (timeout_minutos * 60)
        intervalo_verificacao = 15   

        print(f"Aguardando aparecimento de remanejamentos (timeout: {timeout_minutos} minutos)...")

        while time.time() < tempo_limite:
            remanejamentos = self.verificar_remanejamentos()
            quantidade_atual = len(remanejamentos) if remanejamentos else 0

            if quantidade_atual > 0:
                print(f"{quantidade_atual} remanejamento(s) encontrado(s) após {int(time.time() - tempo_inicio)} segundos")

                ids_remanejamentos = [str(r.get('IDREMANEJAMENTO')) for r in remanejamentos if r.get('IDREMANEJAMENTO')]

                if ids_remanejamentos:
                    print("Iniciando finalização imediata dos remanejamentos encontrados...")
                    self.finalizar_remanejamentos(ids_remanejamentos)
                    return True   

            else:
                tentativa = int((time.time() - tempo_inicio) / intervalo_verificacao) + 1
                print(f"Nenhum remanejamento encontrado (tentativa {tentativa})")

            time.sleep(intervalo_verificacao)

        print(f"Timeout atingido ({timeout_minutos} minutos) sem encontrar remanejamentos")
        return False

    def deletar_avaria(self, id_avaria):
        """Versão otimizada para deletar avarias com múltiplas tentativas"""
        print(f"Iniciando processo de exclusão da avaria ID: {id_avaria}")
        
        payload = {
            "entity": {
                "id": id_avaria,
                "armazemOrigem": None,
                "idLocalOrigem": None,
                "armazemDestino": None,
                "idLocalDestino": None,
                "listaControleAvariaLote": [],
                "usuario": {
                    "id": self.id_token,
                    "nomeUsuario": self.usuario,
                    "senha": self.token_senha,
                    "ativo": True
                }
            },
            "id": None,
            "tela": "Controle de Avaria",
            "usuario": {
                "id": self.id_token,
                "nomeUsuario": self.usuario,
                "senha": self.token_senha,
                "ativo": True
            }
        }

        try:
            print("Tentativa 1: DELETE direto")
            response = self.session.delete(
                f"{self.BASE_URL}/ControleAvariaCRUD/delete",
                json=payload,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 204:
                print(f"Avaria {id_avaria} deletada com sucesso!")
                return True
            else:
                print(f"Tentativa 1 falhou: {response.status_code} - {response.text[:200]}...")
        except Exception as e:
            print(f"Erro na Tentativa 1: {str(e)}")

        try:
            print("Tentativa 2: PUT")
            response = self.session.put(
                f"{self.BASE_URL}/ControleAvariaCRUD/delete",
                json=payload,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 204:
                print(f"Avaria {id_avaria} deletada com sucesso!")
                return True
            else:
                print(f"Tentativa 2 falhou: {response.status_code} - {response.text[:200]}...")
        except Exception as e:
            print(f"Erro na Tentativa 2: {str(e)}")

        raise Exception(f"Não foi possível deletar a avaria {id_avaria} após 2 tentativas")
    
    def processar_avaria_completa(self):
        """Fluxo principal com tratamento de erros aprimorado"""
        id_avaria = None
        try:
            self.login()
            
            print("Criando nova avaria...")
            id_avaria = self.criar_avaria()
            print(f"Avaria criada com sucesso! ID: {id_avaria}")
            
            print("Consultando lotes para avaria...")
            lotes = self.obter_lotes_avaria(id_avaria)
            
            if not lotes:
                print("Nenhum lote válido encontrado. Iniciando rollback...")
                time.sleep(2)
                self.deletar_avaria(id_avaria)
                return False
            
            total_processados = self.adicionar_lotes_avaria(id_avaria, lotes)
            
            if total_processados == 0:
                print("Nenhum lote foi processado com sucesso. Iniciando rollback...")
                self.deletar_avaria(id_avaria)
                return False
            
            self.finalizar_avaria(id_avaria)
            print(f"Avaria concluída! Itens processados: {total_processados}")
            
            self.esperar_e_processar_remanejamentos()
            return True

        except Exception as e:
            print(f"Ocorreu um erro crítico: {str(e)}")
            if id_avaria:
                print("Iniciando rollback...")
                try:
                    self.deletar_avaria(id_avaria)
                except Exception as delete_error:
                    print(f"Falha crítica no rollback: {str(delete_error)}")
                    print("Avaria pode ter ficado órfã no sistema!")
                    
            return False
    
if __name__ == "__main__":
    manager = AvariaManager()
    try:
        success = manager.processar_avaria_completa()
        if success:
            print("Processo concluído com sucesso!")
        else:
            print("Ocorreu um erro durante o processamento")
    except Exception as e:
        print(f"Erro não tratado: {str(e)}")
        success = False
    
    exit(0 if success else 1)