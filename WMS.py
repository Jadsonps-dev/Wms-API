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
