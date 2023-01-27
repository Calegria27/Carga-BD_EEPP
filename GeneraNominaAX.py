import pyodbc
import pandas as pd
import numpy as np
from Google import create_service

##conexión a BD
DRIVER_NAME ='SQL SERVER'
SERVER_NAME = 'iloca'
DATABASE_NAME = 'UNYSOFT'
UID='consultailoca'
PWD='2018iloca'; 

print("Ingrese Año")
año=input()
print("Ingrese Mes")
mes=input()
print("Ingrese Dia")
dia=input()


try:
    conn = pyodbc.connect('DRIVER='+DRIVER_NAME+';SERVER='+SERVER_NAME+';DATABASE='+DATABASE_NAME+';ENCRYPT=no;UID='+UID+';PWD='+ PWD +'')
    print('Connection created') 
except pyodbc.DatabaseError as e:
    print('Database Error 1:')
    print(str(e.value[1]))
except pyodbc.Error as e:
    print('Connection Error 2:')
    print(str(e.value[1]))

cursor = conn.cursor()
cursor.execute("SELECT dbo.INDEPContratoCAB.CtoEmpresa, dbo.maeEmpresa.empNombre, dbo.INDEPContratoCAB.CtoCodigo, dbo.INDMaeUNegocioActivas.CtoDescripcion, cast(dbo.INDEPContratoCAB.NumeroContrato as int),dbo.INDEPContratoCAB.Descripcion, dbo.INDEPAvanceCAB.NumeroPago, dbo.INDEPContratoCAB.RutContratista, dbo.INDTrContratistas.RazonSocial, cast(dbo.INDEPAvanceCAB.NetoDocumento as int),cast(dbo.INDEPAvanceCAB.ValorImpuesto1 as int), cast(dbo.INDEPAvanceCAB.TotalTotal as int), cast(dbo.INDEPAvanceCAB.TotalaPago as int), dbo.INDEPAvanceCAB.VBdo, dbo.INDEPAvanceCAB.VBao, dbo.INDEPAvanceCAB.VBsc, dbo.INDEPAvanceCAB.VBct,dbo.INDEPAvanceCAB.FechaContable, dbo.INDEPAvanceCAB.CodDocCobro, cast(dbo.INDEPAvanceCAB.Nfactura as int), cast(dbo.INDEPAvanceCAB.RetencionEjecucionValor as int), cast(dbo.INDEPAvanceCAB.descanticipovalor as int), dbo.INDEPContratoCAB.plncodigo, dbo.INDEPAvanceCAB.AnoControl, dbo.INDEPAvanceCAB.MesControl, dbo.INDEPAvanceCAB.MovControl, dbo.INDEPAvanceCAB.oc  FROM         dbo.INDEPAvanceCAB INNER JOIN dbo.INDEPContratoCAB ON dbo.INDEPAvanceCAB.IdContrato = dbo.INDEPContratoCAB.IdContrato INNER JOIN dbo.INDTrContratistas ON dbo.INDEPContratoCAB.RutContratista = dbo.INDTrContratistas.RutContratista INNER JOIN dbo.INDMaeUNegocioActivas ON dbo.INDEPContratoCAB.CtoEmpresa = dbo.INDMaeUNegocioActivas.CtoEmpresa AND dbo.INDEPContratoCAB.CtoCodigo = dbo.INDMaeUNegocioActivas.CtoCodigo INNER JOIN dbo.maeEmpresa ON dbo.INDEPContratoCAB.CtoEmpresa = dbo.maeEmpresa.empCodigo WHERE     dbo.INDEPAvanceCAB.oc like 'AOC%' and (VBct is null or VBct ='FACTAX' ) and dbo.INDEPAvanceCAB.AnoControl=('"+str(año)+"') and dbo.INDEPAvanceCAB.MesControl=('"+str(mes)+"') and dbo.INDEPAvanceCAB.MovControl=('"+str(dia)+"')")
data=cursor.fetchall()
data_EEPP=pd.DataFrame(np.array(data)) 

"""
Getting  Google Sheets
"""
CLIENT_SECRET_FILE = 'client_secret.json'
API_NAME = 'sheets'
API_VERSION = 'v4'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

service = create_service(CLIENT_SECRET_FILE, API_NAME, API_VERSION,SCOPES )

google_sheets_id = '15konHY_RcXRBl_oEePTnzmusoRAUSm5OHGq-HXTfDf0'

def construct_request_body(value_array, dimension: str='ROWS') -> dict:
    try:
        request_body = {
            'majorDimension': dimension,
            'values': value_array
        }
        return request_body
    except Exception as e:
        print(e)
        return {}

response = service.spreadsheets().values().get(
    spreadsheetId=google_sheets_id,
    majorDimension='ROWS',
    range='BD_EEPP_REGULAR!A2:AA'
    ).execute()


recordset = data_EEPP.values.tolist()

"""
Insert rows
"""
request_body_values = construct_request_body(recordset)
service.spreadsheets().values().clear(spreadsheetId=google_sheets_id, range='BD_EEPP_REGULAR!A2:AA').execute()
service.spreadsheets().values().update(
    spreadsheetId=google_sheets_id,
    valueInputOption='USER_ENTERED',
    range='BD_EEPP_REGULAR!A2:AA',
    body=request_body_values
    ).execute()

print('Task is complete')

cursor.close()