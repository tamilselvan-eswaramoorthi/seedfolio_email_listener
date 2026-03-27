import base64
import json
import os
from fastapi import FastAPI, Request, Response, BackgroundTasks
from google.oauth2 import service_account
from googleapiclient.discovery import build

from config import Config
from database import db_handler 
from models import EmailTasks

app = FastAPI()


def get_gmail_service():
    key_dict = json.loads(Config.AUTH_JSON)
        
    scopes = ['https://www.googleapis.com/auth/gmail.readonly']
    
    creds = service_account.Credentials.from_service_account_info(
        key_dict, 
        scopes=scopes
    ).with_subject(Config.USER_EMAIL)
    
    return build('gmail', 'v1', credentials=creds)

def renew_gmail_watch():
    """Triggers the Gmail API watch command"""
    try:
        service = get_gmail_service()
        body = {'topicName': Config.TOPIC_ID, 'labelIds': ['INBOX']}
        result = service.users().watch(userId='me', body=body).execute()
        print(f"Gmail Watch renewed. Expiration: {result.get('expiration')}")
    except Exception as e:
        print(f"Watch renewal failed: {e}")

@app.post("/")
async def pubsub_webhook(request: Request, background_tasks: BackgroundTasks):
    """Main entry point for Pub/Sub push notifications"""
    envelope = await request.json()
    
    if not envelope or "message" not in envelope:
        return Response(content="Invalid Pub/Sub message", status_code=400)

    msg = envelope["message"]
    
    # 1. Decode Gmail payload
    try:
        data_str = base64.b64decode(msg["data"]).decode("utf-8")
        data = json.loads(data_str)
        
        new_task = EmailTasks(
            message_id=msg.get("messageId"),
            history_id=str(data.get("historyId")),
            email_address=data.get("emailAddress")
        ) # type: ignore

        # 2. Save to your MSSQL database
        with db_handler.get_session() as session:
            session.add(new_task)
            session.commit()
            
        # 3. Background: Refresh watch every time or conditionally
        background_tasks.add_task(renew_gmail_watch)

        return Response(status_code=200)

    except Exception as e:
        print(f"Error processing webhook: {e}")
        return Response(status_code=200)

@app.get("/health")
def health_check():
    return {"status": "healthy"}
