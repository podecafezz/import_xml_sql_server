import os
import pyodbc
import xml.etree.ElementTree as ET

# CONFIG DB
server = r'Hp-Elitebook\SQLEXPRESS'
database = 'DB_OSMAR_PROJECT'
username = 'devcafe'
password = 'cafe'
driver = '{ODBC Driver 18 for SQL Server}'  


conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

#DIRECTORY
caminho_arquivos = 'seu diretório dos arquivos'

#tratamento da sessões

try:

    for nome_arquivo in os.listdir(caminho_arquivos):
        if nome_arquivo.endswith('.xml'):
            print(f"Processando o arquivo: {nome_arquivo}")
            caminho_completo = os.path.join(caminho_arquivos, nome_arquivo)

            try:

                tree = ET.parse(caminho_completo)
                root = tree.getroot()


                for session in root.findall('.//x:session',
                                            namespaces={'x': 'xxxx'}):
                    session_id = session.get('x:id')


                    cursor.execute(f"DELETE FROM [dbo].[TB_STG_CALL_DATA] WHERE session_id = '{session_id}'")


                    sql = "INSERT INTO [dbo].[TB_STG_CALL_DATA] (session_id"
                    values = f"VALUES ('{session_id}'"

                    for tag in session.findall('.//x:tag', namespaces={'x': 'xxxxxxx'}):
                        for attribute in tag.findall('x:attribute', namespaces={'x': 'xxxxxxxxxxx'}):
                            key = attribute.get('x:key')
                            value = attribute.text
                            sql += f", {key}"
                            values += f", '{value}'"

                    sql += ") "
                    values += ")"
                    cursor.execute(sql + values)
                    conn.commit()
                    print("Registro inserido com sucesso!")

            except Exception as e:
                print(f"Erro ao processar o arquivo {nome_arquivo}: {str(e)}")
                # Desfaz as alterações se ocorrer um erro
                conn.rollback()

except Exception as main_exception:
    print(f"Erro principal: {str(main_exception)}")

finally:
   
    conn.close()
