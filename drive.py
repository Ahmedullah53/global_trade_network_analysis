import os
import json
import requests
from auth import Auth
from dotenv import load_dotenv

class Drive:
    def __init__(self):
        load_dotenv()
        auth = Auth()
        auth.generate_token()
        self.headers = {"Authorization": f"Bearer {os.getenv('AUTH_KEY')}"}

    def upload(self, filename:str)-> None:
        para = {
            "name": filename,
            "parents": [f"{os.getenv('DATA')}"]
        }
        files = {
            'data': ('metadata', json.dumps(para), 'application/json; charset=UTF-8'),
            'file': open(f"./{filename}", "rb")
        }
        r = requests.post(
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
            headers=self.headers,
            files=files
        )
        print(r.text)
        return r.text