# Classe: awsTextractWrapper

# Importação das Bibliotecas:
import boto3
import botocore
import pandas as pd
import os

# Declaração da Classe Principal
class awsTextractWrapper:
    """
    ### Classe: awsTextractWrapper

    Implementação para utilização do serviço AWS Textarct para Analize de Imagem. <br>
    O objetivo é facilitar o acesso aos dados de extração realizados pelo serviço AWS.
    
    by Sérgio C. Medina
    """

    # Metodo inicial de definição
    def __init__(self, debugMode):
        """
        Metodo inicial de definição

        Parametro:
        - debugMode: (False = debug)
        """        
        self.__debugMode = debugMode if debugMode is not None else debugMode

        # Limpando as variaveis em memória
        self.__clearAll()


    def __clearAll(self):
        """
        Metodo utilizado para limpar o conteúdo dos elementos de memória
        mais importantes do processamento

        Parametro:
        - None
        """

        # Parametros AWS
        self.__aws_service  = 'textract'
        self.__aws_region_name  = None
        self.__aws_access_key_id = None
        self.__aws_secret_access_key = None

        # Client AWS Boto3
        self.__client = None
        self.__clientResp = None

        # Document 
        self.__filepath = None
        self.df_tables = None
        self.key_values = None



    def getClientAWS(self):
        """
        Metodo utilizando para retornar o Client AWS para o Serviço Textract, 
        caso seja a primeiro chamada o Client é criado, não sendo a primeira
        chamado o mesmo será reutilizado.

        Parametro:
        - None
        """

        if (self.__client is None):
            self.__client = boto3.client(
                self.__aws_service, 
                region_name=self.__aws_region_name, 
                aws_access_key_id=self.__aws_access_key_id, 
                aws_secret_access_key=self.__aws_secret_access_key
            )

        return self.__client        


    def readEnvCredentials(self):
        """
        Metodo para verificar as credenciais de autenticação definidas em 
        variáveis de ambiente do SO, quando não utilizado o AWSCLI.

        Sendo utilizando a função de configuração do AWSCLI, esta função não
        deve ser utilizada, em caso de uso as Variáveis que devem ser definidas 
        no ambiente são: AWS_REGION_NAME, AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY

        Parametro:
        - None
        """

        return self.setEnvCredentials(
            aws_region_name = os.getenv('AWS_REGION_NAME'),
            aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        )

    def setEnvCredentials(self, aws_region_name, aws_access_key_id, aws_secret_access_key):
        """
        Metodo para definir as credenciais de autenticação manualmente quando 
        não utilizado o AWSCLI.

        Sendo utilizando a função de configuração do AWSCLI, esta função não
        deve ser utilizada, esta é uma opção não aplicavel por questões de segurança

        Parametro:
        - aws_region_name: região de processamente da AWS
        - aws_access_key_id: definição do Access Key do usuário na AWS
        - aws_secret_access_key: definição da Secret Key do usuário na AWS
        """                  

        self.__aws_region_name = aws_region_name
        self.__aws_access_key_id = aws_access_key_id
        self.__aws_secret_access_key = aws_secret_access_key

        resultCod   = -1
        resultMsg   = None

        if self.__aws_region_name is None:
            resultMsg   = 'Error: AWS_REGION_NAME not defined.'
        elif self.__aws_access_key_id is None:
            resultMsg   = 'Error: AWS_ACCESS_KEY_ID not defined.'
        elif self.__aws_secret_access_key is None:
            resultMsg   = 'Error: AWS_SECRET_ACCESS_KEY not defined.'
        else:
            resultCod   = 0
            resultMsg   = 'Success.'

        return resultCod, resultMsg


    def __map_blocks(self, blocks, block_type):
        """
        Metodo utilizado buscar dos blocos retornados uma lista e um tipo especificado.

        Parametro:
        - blocks.
        - block_type       
        """        
        return {
            block['Id']: block
            for block in blocks
            if block['BlockType'] == block_type
        }


    def __get_children_ids(self, block):
        """
        Metodo para retornar os IDs dos CHILD, de um determinado bloco.

        Parametro:
        - blocks.
        - block_type       
        """         
        for rels in block.get('Relationships', []):
            if rels['Type'] == 'CHILD':
                yield from rels['Ids']


    def __map_Tables(self):
        """
        Metodo utilizado para analizar o retorno e organizar as
        estruturas de TABLES reconhecidas pelo Textract em
        DataFrames.

        Parametro:
        - None.        
        """

        blocks = self.__clientResp['Blocks']
        tables = self.__map_blocks(blocks, 'TABLE')
        cells = self.__map_blocks(blocks, 'CELL')
        words = self.__map_blocks(blocks, 'WORD')
        selections = self.__map_blocks(blocks, 'SELECTION_ELEMENT')

        self.df_tables = []

        for table in tables.values():

            # Determine all the cells that belong to this table
            table_cells = [cells[cell_id] for cell_id in self.__get_children_ids(table)]

            # Determine the table's number of rows and columns
            n_rows = max(cell['RowIndex'] for cell in table_cells)
            n_cols = max(cell['ColumnIndex'] for cell in table_cells)
            content = [[None for _ in range(n_cols)] for _ in range(n_rows)]

            # Fill in each cell
            for cell in table_cells:
                cell_contents = [
                    words[child_id]['Text']
                    if child_id in words
                    else selections[child_id]['SelectionStatus']
                    for child_id in self.__get_children_ids(cell)
                ]
                i = cell['RowIndex'] - 1
                j = cell['ColumnIndex'] - 1
                content[i][j] = ' '.join(cell_contents)

            # We assume that the first row corresponds to the column names
            df = pd.DataFrame(content[1:], columns=content[0])
            self.df_tables.append(df)


    def __map_KeyValues(self):
        """
        Metodo utilizado para analizar o retorno de buscar
        a lista de Chave-Valor localizado no documento.

        Parametro:
        - None.        
        """

        key_map, value_map, block_map = self.__get_kv_map(self.__clientResp)
        self.key_values = self.__get_kv_relationship(key_map, value_map, block_map)


    def __get_kv_map(self, response):

        # Get the text blocks
        blocks=response['Blocks']

        # get key and value maps
        key_map = {}
        value_map = {}
        block_map = {}
        for block in blocks:
            block_id = block['Id']
            block_map[block_id] = block
            if block['BlockType'] == "KEY_VALUE_SET":
                if 'KEY' in block['EntityTypes']:
                    key_map[block_id] = block
                else:
                    value_map[block_id] = block

        return key_map, value_map, block_map


    def __get_kv_relationship(self, key_map, value_map, block_map):
        kvs = {}
        for block_id, key_block in key_map.items():
            value_block = self.__find_kv_value_block(key_block, value_map)
            key = self.__get_kv_text(key_block, block_map)
            val = self.__get_kv_text(value_block, block_map)
            kvs[key] = val
        return kvs


    def __find_kv_value_block(self, key_block, value_map):
        for relationship in key_block['Relationships']:
            if relationship['Type'] == 'VALUE':
                for value_id in relationship['Ids']:
                    value_block = value_map[value_id]
        return value_block


    def __get_kv_text(self, result, blocks_map):
        text = ''
        if 'Relationships' in result:
            for relationship in result['Relationships']:
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        word = blocks_map[child_id]
                        if word['BlockType'] == 'WORD':
                            text += word['Text'] + ' '
                        if word['BlockType'] == 'SELECTION_ELEMENT':
                            if word['SelectionStatus'] == 'SELECTED':
                                text += 'X'    

                                    
        return text


    def processLocalDoc(self, filepath):
        """
        Metodo utilizado para processar cada documento.

        Parametro:
        - filepath: caminho e nome do documento no padrão do OS.        
        """ 

        resultCod   = -1
        resultMsg   = None

        try:
            # Lendo o arquivo informado
            with open(filepath, 'rb') as locaFile:
                doc = bytearray(locaFile.read())

                try:
                    # Enviando o arquivo para processamento na AWS
                    self.getClientAWS()
                    self.__clientResp = self.__client.analyze_document(
                        Document={'Bytes': doc},
                        FeatureTypes=["TABLES"]
                    )

                    # Separando os dados em Tabelas
                    self.__map_Tables()

                    # Separando os dados de Chave-Valor
                    self.__map_KeyValues()

                    resultCod   = 0
                    resultMsg   = f'AWS - Doc processed.'  

                except botocore.exceptions.ClientError as error:
                    resultCod   = -1
                    resultMsg   = f'AWS ClientError: {error}'                

                except botocore.exceptions.ParamValidationError as error:       
                    resultCod   = -1
                    resultMsg   = f'AWS ParamValidationError: {error}'

                locaFile.close()
        
        except IOError as error:
            resultCod   = -1
            resultMsg   = f'FILE Open Error: {error}'            

        return resultCod, resultMsg




