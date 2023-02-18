""" from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
 """
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

API_SERVICE_NAME_SHEETS = 'sheets'
API_VERSION_SHEETS = 'v4'

API_SERVICE_NAME_DRIVE = 'drive'
API_VERSION_DRIVE = 'v3'

CLIENT_SECRETS_PATH = 'ADD YOUR PATH\\02_data_pipeline\\scripts\\_0_auth\\secrets_and_tokens\\secrets_a.json'
CREDENTIAL_PATH = 'ADD YOUR PATH\\02_data_pipeline\\scripts\\_0_auth\\secrets_and_tokens\\token_a.json'


# authentifizierung und credentials speichern in saved_credentials.json
def get_auth_a_sheets():
    store = Storage(CREDENTIAL_PATH)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRETS_PATH, SCOPES)
        credentials = tools.run_flow(flow, store)
    return build(API_SERVICE_NAME_SHEETS, API_VERSION_SHEETS, credentials=credentials)


def get_auth_a_drive():
    store = Storage(CREDENTIAL_PATH)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRETS_PATH, SCOPES)
        credentials = tools.run_flow(flow, store)
    return build(API_SERVICE_NAME_DRIVE, API_VERSION_DRIVE, credentials=credentials)
