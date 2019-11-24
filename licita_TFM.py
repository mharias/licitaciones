# Descarga diariamente el fichero del portal de licitaciones
# Prepara un pandas de adjudicaciones y licitaciones que
# sube a un BigQuery
# Una DashBoard DataStudio o similar presenta los datos

import xml.etree.ElementTree as ET  
import re
import urllib3
import pandas as pd
import certifi
from datetime import datetime,date,timedelta
import smtplib
import pandas as pd
from google.cloud import bigquery
import os
import sys

def root2pandas(root,CPVs,status_licitacion,ns):
   """Rutina para convertir un root de XML en Pandas

    Parameters:
    root: enlace al XML
    CPVs: lista de codigo
    status_licitacion: código de estado de la licitación para el filtro: PUB como publicado y ADJ como Adjudicado
    ns: Atributo namespace para la correcta decodificación del XML
    Returns:
    pandas con los campos filtrados y con las columnas elegidas según el status_licitacion

   """

    columnas={'ADJ':['id','fecha_publicacion','title','status','link',
                                      'entidad','poblacion','provincia','fecha_adjudicado',
                                      'capitulo','ganador_id','ganador','importe_pto',
                                      'importe_won','plazo','plazo_unit'],
             'PUB':['id','fecha_publicacion','title','status','fecha_entrega','link',
                                      'entidad','poblacion','provincia','capitulo','importe_pto','plazo','plazo_unit']
             }
    
    lista=[]
    for a in root.findall('./prefix:entry',ns):
    #for a in root.findall('./prefix:entry//*[cbc-place-ext:ContractFolderStatusCode="ADJ"]/..',ns):
        entrada={}
        try:
            fecha_publicacion=a.find('./prefix:updated',ns).text
        except:
            fecha_publicacion='N/A'
        
        try:
            id_licitacion=a.find('./prefix:id',ns).text
        except:
            id_licitacion='N/A'
        
        try:
            status=a.find('.//cbc-place-ext:ContractFolderStatusCode',ns).text
        except:
            status='N/A'
        
        try:
            capitulo=a.find('.//cbc:ItemClassificationCode',ns).text
        except:
            capitulo='00'
        
        try:
            title=a.find('.//cac:ProcurementProject/cbc:Name',ns).text
        except:
            title='N/A'
        
        try:
            link=a.find('./prefix:link',ns).attrib['href']
        except:
            link='N/A'
        
        try:
            entidad=a.find('.//cac:PartyName/cbc:Name',ns).text
        except:
            entidad='N/A'
        
        try:
            poblacion=a.find('.//cbc:CityName',ns).text
        except:
            poblacion='N/A'
        
        try:
            provincia=a.find('.//cbc:CountrySubentity',ns).text
        except:
            provincia='N/A'
            
        try:
            fecha_entrega=a.find('.//cbc:EndDate',ns).text
            
        except:
            fecha_entrega=date(1,1,1)
        
        
        try:
            #fecha=a.find('.//cbc:EndDate',ns).text
            fecha_adjudicacion=a.find('.//cbc:AwardDate',ns).text
        except:
            fecha_adjudicacion='N/A'
            
        try:
            #elemento.find('.//cac:TenderResult/cac:WinningParty/cac:PartyName/cbc:Name',ns).text)
            winner=a.find('.//cac:TenderResult/cac:WinningParty/cac:PartyName/cbc:Name',ns).text
        except:
            winner='N/A'
            
        try:
            importe_pto=a.find('.//cbc:TaxExclusiveAmount',ns).text
        except:
            importe_pto=0
        
        try:
            importe_won=a.find('.//cac:AwardedTenderedProject/cac:LegalMonetaryTotal/cbc:TaxExclusiveAmount',ns).text
        except:
            importe_won=0
        try:
            plazo=a.find('.//cac:PlannedPeriod/cbc:DurationMeasure',ns).text
        except:
            plazo='N/A'
        try:
            plazo_unit=a.find('.//cac:PlannedPeriod/cbc:DurationMeasure',ns).attrib['unitCode']
        except:
            plazo_unit='N/A'
        try:
            ganador_id=a.find('.//cac:TenderResult/cac:WinningParty/cac:PartyIdentification/cbc:ID',ns).text
        except:
            ganador_id='N/A'
            
        if status==status_licitacion and capitulo[0:2] in CPVs:
        
            entrada['id']=id_licitacion
            entrada['status']=status
            entrada['capitulo']=capitulo
            entrada['provincia']=provincia
            entrada['poblacion']=poblacion
            entrada['entidad']=entidad
            entrada['link']=link
            entrada['title']=title
            entrada['plazo']=plazo
            entrada['plazo_unit']=plazo_unit
            entrada['fecha_publicacion']=fecha_publicacion
            entrada['importe_pto']=importe_pto
            if status=='PUB':
                entrada['fecha_entrega']=fecha_entrega
                #print ('ok fecha')
            
            if status=='ADJ':
                entrada['fecha_adjudicado']=fecha_adjudicacion
                entrada['ganador']=winner
                entrada['importe_won']=importe_won
                entrada['ganador_id']=ganador_id
            
            lista.append(entrada)
    
    panda=pd.DataFrame(lista,columns=columnas[status_licitacion])
    
    
    return panda





def web2root(path,fecha_inicio,ns):
    """Función para generar un root desde el fichero del portal
    Parameters:
    path: enlace al XML
    fecha_inicio: fecha de inicio del informe. El resultado tendrá todos los campos desde 
    esa fecha
    ns: Atributo namespace para la correcta decodificación del XML
    Returns:
    root con los todos los campos del repositorio del portal
    """

    lista_root=[]
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
    #http = urllib3.PoolManager()
    response = http.request('GET', path)
    root=ET.fromstring(response.data)
    
    fecha=root.find('.prefix:updated',ns).text
    fecha=datetime.strptime(fecha[0:10],'%Y-%m-%d').date()
    #bucle en el que se bajan todos los fecheros diarios hasta llegar a fecha_inicio
    nuevo_path=path
    while fecha>=fecha_inicio:
        #print (fecha,'\n',nuevo_path)
        lista_root.append(root)
        #print('Tratando {} de fecha {}'.format(nuevo_path,fecha.strftime('%d/%m')))
        nuevo_path=root.find('.prefix:link/[@rel="next"]',ns).attrib['href']
        response = http.request('GET', nuevo_path)
        root=ET.fromstring(response.data)
        fecha=datetime.strptime(root.find('.prefix:updated',ns).text[0:10],'%Y-%m-%d').date()
        
    return lista_root

def ajustes_formato(pandas,tipo):
    """Rutina para ajustar ciertos formatos del pandas, devuelve un pandas con los campos ajustados
    Parameters:
    pandas: pandas con los datos a formatear
    tipo: N/A...sin uso en la nueva versión
    Returns:
    pandas mismo tipo que la entrada con los campos ajustados
    """

    panda_aux=pandas
    panda_aux.drop_duplicates(['id'],'first',inplace=True)
    try:
        panda_aux['fecha_vto']=pd.to_datetime(panda_aux['fecha_vto'],format='%Y-%m-%d').dt.date
        #print ('OK vto')
    except:
        panda_aux['fecha_vto']='N/A'
    
    try:
        panda_aux['fecha_publicacion']=pd.to_datetime(panda_aux['fecha_publicacion'],format='%Y-%m-%d').dt.date
        #print ('OK publicacion')
    except:
        panda_aux['fecha_publicacion']='N/A'
    
    panda_aux['importe_pto']=pd.to_numeric(panda_aux['importe_pto'])
    panda_aux['title']=panda_aux['title'].apply(lambda x:x.replace('\n',' '))
    panda_aux['title']=panda_aux['title'].apply(lambda x:x.replace(';',','))
    
    
    if tipo=='PUB':
        try:
            panda_aux['fecha_entrega']=pd.to_datetime(panda_aux['fecha_entrega'],format='%Y-%m-%d').dt.date
            #print ('OK entrega')
        except:
            panda_aux['fecha_entrega']=date(1,1,1)
    
    if tipo=='ADJ':
        try:
            panda_aux['fecha_adjudicado']=pd.to_datetime(panda_aux['fecha_adjudicado'],format='%Y-%m-%d').dt.date
            #print ('OK adjudicado')
        except:
            panda_aux['fecha_adjudicado']='N/A'
    
        panda_aux['importe_won']=pd.to_numeric(panda_aux['importe_won'])
        panda_aux['ganador_id']=panda_aux['ganador_id'].apply(lambda x : x.upper())
#panda_aux.dropna(subset=['plazo'],inplace=True)
    panda_aux.set_index('id',inplace=True)
    return panda_aux

def annade_vto(pandas):
    """Rutina que calcula la fecha de vencimiento de las licitaciones y la añade como un campo adicional
    Parameters:
    pandas: panda con el pandas a tratar
    Returns:
    Devuelve el mismo pandas con la fecha de vencimiento añadida, en el caso de que fecha no se pueda calcular
    devolverá la fecha de adjudicación
    """
    def fecha_vto(row):
        """Devuelve una fecha equivalente a la fecha de vencimiento, calculada añadiendo plazo_unit a la fecha
        de adjudicación
        Parameters:
        pandas: panda con el pandas a tratar
        Returns:
        Devuelve el mismo pandas con la fecha de vencimiento añadida, en el caso de que fecha no se pueda calcular
        devolverá la fecha de adjudicación
        """

        tags={'MON':30.4,'ANN':365,'DAY':1}
        try:
            fecha_=row['fecha_adjudicado']+timedelta(days=tags[row['plazo_unit']]*int(row['plazo']))
        except:
            fecha_= row['fecha_adjudicado']
        return fecha_


    panda_aux=pandas
    panda_aux['fecha_adjudicado'] = pd.to_datetime(panda_aux['fecha_adjudicado'], format='%Y-%m-%d')
    panda_aux['fecha_vto']=panda_aux.apply(lambda row:fecha_vto(row),axis=1)
    panda_aux['fecha_vto'] = pd.to_datetime(panda_aux['fecha_vto'], format='%Y-%m-%d %H:%M:%S')
    return panda_aux

def annade_bajada(pandas):
    """Rutina que calcula la bajada porcentual realizada en la licitación
    Parameters:
    pandas: panda con el pandas a tratar
    Returns:
    Devuelve el mismo pandas con la bajada calculada añadida
    devolverá la fecha de adjudicación
    """
    panda_aux=pandas
    panda_aux['importe_pto']=pd.to_numeric(panda_aux['importe_pto'])
    panda_aux['importe_won']=pd.to_numeric(panda_aux['importe_won'])
    panda_aux['bajada']=1-panda_aux['importe_won']/panda_aux['importe_pto']
    #panda_aux['bajada']=panda_aux['bajada'].astype(float).map(lambda n:'{:.2%}'.format(n))
    return panda_aux

def bq2pandas(sql):
     """
    Rutina que ejecuta una query sql para devolver el pandas de la tabla BigQuery
    Parameters:
    pandas: panda con el pandas a tratar
    Returns:
    Devuelve el mismo pandas con la fecha de vencimiento añadida, en el caso de que fecha no se pueda calcular
    devolverá la fecha de adjudicación
    """

    #project='prueba-python-218008'
    #dataset_id = 'Pruebas'
    #table_id = 'adjudicaciones'
    client = bigquery.Client()

    #sql = "SELECT * FROM "+"`"+project+"."+dataset_id+"."+table_id+"`"
    sql_=sql
    try:    
        df = client.query(sql_).to_dataframe()
        df['ganador_id']=df['ganador_id'].apply(lambda x:x.upper())
        df.set_index('ganador_id',inplace=True)
    except:
    #error=sys.exc_info()
        df=pd.DataFrame()
        print ('problemas para bajar la tabla de Bigquery',sys.exc_info()[1])
    
    return df

def normaliza_ganador(panda,project,dataset_id,table_id):
    """
    Rutina que normaliza el nombre del ganador ya que se introduce de diversas
    maneras, se una el cif con clave.
    Parameters:
    pandas: panda con los registros a tratar
    project, dataset_id,table_id: terna con el identificativo de la tabla en Google
    Returns:
    Devuelve el mismo pandas con los nombres normalizados
    """

    panda_aux=panda
    panda_aux['ganador_id']=panda_aux['ganador_id'].apply(lambda x : x.upper())
    cifs=panda_aux.groupby(['ganador_id'])[['ganador_id','ganador']].head().sort_values('ganador_id')
    cifs.drop_duplicates(['ganador_id'],'first',inplace=True)
    #cifs['ganador_id']=cifs['ganador_id'].apply(lambda x : x.upper())
    cifs.set_index('ganador_id',inplace=True)

    # from google.cloud import bigquery
    sql_bajada_cifs = "SELECT * FROM "+"`"+project+"."+dataset_id+"."+table_id+"`"
    df=bq2pandas(sql_bajada_cifs)
    
    cifs_reset=cifs.reset_index()
    df_reset=df.reset_index()
    resultado=pd.concat([df_reset,cifs_reset],ignore_index=True,sort=True)
    resultado.drop_duplicates(['ganador_id'],'first',inplace=True)
    resultado.set_index('ganador_id',inplace=True)
    
    panda_aux['ganador_normalizado']=panda_aux['ganador_id'].apply(lambda x:resultado.loc[x])
    #actualiza la tabla cifs
    nuevo_cifs=panda_aux.groupby(['ganador_id'])[['ganador_id','ganador']].head().sort_values('ganador_id')
    nuevo_cifs.drop_duplicates(['ganador_id'],'first',inplace=True)
    #cifs['ganador_id']=cifs['ganador_id'].apply(lambda x : x.upper())
    #nuevo_cifs.set_index('ganador_id',inplace=True)

    #sql_borrado_cifs = "DELETE FROM "+"`"+project+"."+dataset_id+"."+table_id+"` WHERE True"
    #borra la tabla CIFS existente en BQ
    borra_tabla(dataset_id,table_id)
    #la actualiza con la nueva tabla
    pandas2bq(resultado,project,dataset_id,table_id)
    return panda_aux

def borra_tabla(dataset_id,table_id):
    """
    Rutina para borrar una tabla de BigQuery
    Parameters:
    dataset_id, table_id: id de la tabla
    Returns:
    """	
	client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    client.delete_table(table_ref)  # API request
    print('Table {}:{} deleted.'.format(dataset_id, table_id))
    return


def pandas2bq(panda_,project,dataset_id,table_id):
    """
    Rutina para subir un pandas a una tabla BigQuert
    Parameters:
    panda_: panda a subir
    dataset_id, table_id: id de la tabla
    Returns:
    """	
    location_proj='EU'
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    #job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.autodetect = False
    job = client.load_table_from_dataframe(
        dataframe=panda_, 
        destination=table_ref, 
        location=location_proj, 
        job_config=job_config)
    job.result()  # Waits for table load to complete.

    print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_id, table_id))
    
    
    return 'Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_id, table_id)
    


# programa principal


# Variables globales

# ns: variable con la configuración metadato XML, se saca de la cabecera del fichero XML

ns={'prefix':'http://www.w3.org/2005/Atom',
    'cbc-place-ext':"urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2", 
    'cac-place-ext':"urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2",
    'cbc':"urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2",
    'cac':"urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2",
    'ns1':"urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2"}

# dos primeros dígitos de los capítulos de interés, actualizar con el suyo
CPV_interes=['32','48','64','72','73'] 

# path de localiación del portal con los fichero
path1='https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom'
path2='https://contrataciondelestado.es/sindicacion/sindicacion_1044/PlataformasAgregadasSinMenores.atom'


hoy=date.today()

# configura el entorno para poder trabajar con GCP, actualizar con el suyo
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='path a fichero con las claves GCP'

# nombre del proyecto y del dataset en GCP. Actualizar con el suyo
project='nombre_del_proyecto'
dataset_id = 'Pruebas'

#Extract
root_link=web2root(path1,hoy,ns)

#Transform Adjudicaciones
panda_ADJ=pd.DataFrame()
for root_ in root_link:
    panda_=root2pandas(root_,CPV_interes,'ADJ',ns)
    panda_ADJ = pd.concat([panda_ADJ, panda_], ignore_index=True, sort=False)
panda_ADJ=annade_vto(panda_ADJ)
panda_ADJ=annade_bajada(panda_ADJ)


panda_ADJ=normaliza_ganador(panda_ADJ,project,dataset_id,'cifs')
panda_ADJ=ajustes_formato(panda_ADJ,'ADJ')

#Load ADjudicaciones
table_id = 'adjudicaciones'
pandas2bq(panda_ADJ,project,dataset_id,table_id)

#Transform Publicaciones
panda_PUB=pd.DataFrame()
for root_ in root_link:
    panda_=root2pandas(root_,CPV_interes,'PUB',ns)
    panda_PUB = pd.concat([panda_PUB, panda_], ignore_index=True, sort=False)
panda_PUB=ajustes_formato(panda_PUB,'PUB')


#Load Publicaciones
table_id = 'publicaciones'
pandas2bq(panda_PUB,project,dataset_id,table_id)