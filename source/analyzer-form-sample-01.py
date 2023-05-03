# Analise do FORMULARIO 1 para testes

# Imports
import boto3

# 
# ATENÇÃO: O uso de credenciais fixos nos códigos NÃO é uma boa prática de segurança 
# e devemos evitar expor as credenciais, pois os serviços em nuvem são tarifados, no 
# código abaixo só está sendo utilizado por ser um caso especificamente para estudos, 
# então busque seguir a recomendação de segurança da AWS verificar a documentação em:
#
# https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/configuration.html
#
ACCESS_KEY='[ACCESS_KEY]'
SECRET_KEY='[SECRET_KEY]'

#
# Formulario de exemplo
#
FORM_IMG = '../data/images/form-sample-01'

# Declaração AWS Boto3 Client - Textarct
client = boto3.client(
    'textract', 
    region_name='us-west-2', 
    aws_access_key_id=ACCESS_KEY, 
    aws_secret_access_key=SECRET_KEY
)

# Lendo a imagem
with open(FORM_IMG, 'rb') as document:
    img = bytearray(document.read())
    document.close()

# >> Call Amazon Textract
response = client.detect_document_text(
    Document={'Bytes': img}
)



