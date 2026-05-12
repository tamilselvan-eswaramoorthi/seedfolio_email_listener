import json
from fastapi import FastAPI, Request, Response, BackgroundTasks
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlmodel import select

from config import Config
from database import db_handler, EmailTasks
from utilities.gmail import GetHoldingsFromGmail

app = FastAPI()
 
@app.on_event("startup")
def on_startup():
    try:
        db_handler.create_db_and_tables()
        print("Database initialized successfully.")
    except Exception as e:
        print(f"Database initialization failed: {e}")

def get_gmail_service():
    if isinstance(Config.AUTH_JSON, str):
        key_dict = json.loads(Config.AUTH_JSON)
    else:
        key_dict = Config.AUTH_JSON
        
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


@app.post("/process")
async def process_webhook(request: Request, background_tasks: BackgroundTasks):
    """Main entry point for apps Script calls"""
    payload = await request.json()
    
    if not payload:
        return Response(content="Invalid request", status_code=400)

    # 1. Handle Direct Message ID (e.g., from Google Apps Script)
    if "messageId" in payload:
        message_id = payload["messageId"]
        print(f"Received direct request for message ID: {message_id}")
        
        with db_handler.get_session() as session:
            # Check if already processed
            task_exists = session.exec(select(EmailTasks).where(EmailTasks.message_id == message_id)).first()
            if task_exists and task_exists.status == "COMPLETED":
                print(f"Message ID {message_id} already processed.")
                return Response(status_code=200)

            if not task_exists:
                task = EmailTasks(message_id=message_id, status="PROCESSING")
                session.add(task)
                session.commit()

        # Process outside of the initial session to avoid keeping transaction open
        gmail = GetHoldingsFromGmail()
        ret = gmail.process_transactions(message_id)
        print(f"Processing result for message ID {message_id}: {ret}")

        with db_handler.get_session() as session:
            task = session.exec(select(EmailTasks).where(EmailTasks.message_id == message_id)).first()
            if task:
                task.status = "COMPLETED"
                session.add(task)
                session.commit()
        background_tasks.add_task(renew_gmail_watch)
        return Response(status_code=200)
    else:
        return Response(content="Unsupported payload format", status_code=400)

@app.get("/health")
def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)