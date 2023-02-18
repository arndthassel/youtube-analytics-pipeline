""" from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
 """
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

SCOPES = ['https://www.googleapis.com/auth/yt-analytics.readonly',
          'https://www.googleapis.com/auth/yt-analytics-monetary.readonly']

API_SERVICE_NAME = 'youtubeAnalytics'
API_VERSION = 'v2'

CLIENT_SECRETS_PATH = 'ADD YOUR PATH\\02_data_pipeline\\_0_auth\\secrets_tb.json'
CREDENTIAL_PATH = 'ADD YOUR PATH\\02_data_pipeline\\scripts\\_0_auth\\secrets_and_tokens\\token_tb.json'


# authentifizierung und credentials speichern in saved_credentials.json
def get_auth_tb():
    store = Storage(CREDENTIAL_PATH)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRETS_PATH, SCOPES)
        credentials = tools.run_flow(flow, store)
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)
