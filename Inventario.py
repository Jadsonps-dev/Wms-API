import os
import datetime as dt
import sys
import warnings
import importlib

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

class Inventario(Estrutura):
    def __init__(self):
        super().__init__()

        self.data_inicio = '18/06/2025'
        self.data_final = dt.datetime.today().strftime('%d/%m/%Y')
        self.notebook_path = os.path.join(self.google_drive_path, "ProjectsDeA", "Alpargatas", "jupytes", "InventarioNovo.ipynb")
        self.id = 932

        self.save_path = os.path.join(self.google_drive_path, "ProjectsDeA", "Alpargatas", "basesDashboards", "Inventario")

        self.spreadsheet_data = [
            {
                'spreadsheet_id': 'id_sheets',
                'abas_e_csv': {
                    'inventario': os.path.join(self.google_drive_path, "ProjectsDeA", "Alpargatas", "basesDashboards", "Inventario", "Relatório de Inventário.csv"),
                    'tabela': os.path.join(self.google_drive_path, "ProjectsDeA", "Alpargatas", "basesDashboards", "Inventario", "Tabela.csv")
                }
            }
        ]
        
    def executor(self):
        print('Inventario Insider\nHora Inicio:', dt.datetime.now().strftime("%H:%M:%S"))
        self.extrair_dados_relatorios(self.user_wms, self.senha_wms, self.id, self.data_inicio, self.data_final, self.save_path)
        self.execute_notebook(self.notebook_path)
        self.upload_sheet_Alpargatas(self.spreadsheet_data)
        print('Hora Fim:', dt.datetime.now().strftime("%H:%M:%S"))
        
exe = Inventario()
exe.executor()
