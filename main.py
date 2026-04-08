import base64
import json
import traceback
from fastapi import FastAPI, Request, Response, BackgroundTasks
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlmodel import select

from config import Config
from database import db_handler, User, EmailTasks
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
async def pubsub_webhook(request: Request, background_tasks: BackgroundTasks):
    """Main entry point for Pub/Sub push notifications"""
    envelope = await request.json()
    
    if not envelope or "message" not in envelope:
        return Response(content="Invalid Pub/Sub message", status_code=400)

    msg = envelope["message"]
    
    # 1. Decode Gmail payload
    try:
        if isinstance(msg.get("data"), str):
            data_str = base64.b64decode(msg.get("data")).decode("utf-8")
            data = json.loads(data_str)
        else:
            data = msg.get("data", {})

        gmail = GetHoldingsFromGmail()


        history_id = data.get('historyId')

        with db_handler.get_session() as session:
            last_history_id = session.exec(select(EmailTasks.history_id).order_by(EmailTasks.history_id.desc())).first()
            if last_history_id:
                if int(history_id) < int(last_history_id):
                    print(f"Ignoring message with history ID {history_id} as it's older than last processed history ID {last_history_id}")
                    return Response(status_code=200)
            else:
                results = gmail.service.users().getProfile(userId='me').execute()
                initial_task = EmailTasks(message_id="INITIAL", history_id=results['historyId'], status="INITIAL") #type: ignore
                session.add(initial_task)
                session.commit()
                return Response(status_code=200)
            
            # 2. Get the history record from Gmail
            history = gmail.service.users().history().list(userId='me', startHistoryId=last_history_id).execute()
            message_ids = []
            if 'history' in history:
                for record in history['history']:
                    if 'messagesAdded' in record:
                        for message_item in record['messagesAdded']:
                            message_id = message_item['message']['id']
                            message_ids.append(message_id)
            message_ids = set(message_ids)
            print(f"Found {len(message_ids)} new message IDs in history since last history ID {last_history_id}")
            unprocessed_message_ids = []
            for message_id in message_ids:
                task_exists = session.exec(select(EmailTasks).where(EmailTasks.message_id == message_id)).first()
                if task_exists:
                    continue
                unprocessed_message_ids.append(message_id)

            for message_id in unprocessed_message_ids:
                task = EmailTasks(message_id=message_id, history_id=str(history_id), status="PROCESSING") #type: ignore
                session.add(task) 

                ret = gmail.process_transactions(message_id)
                if ret.get('status') == 202 or ret.get('status') == 500 or ret.get('status') == 200:
                    with db_handler.get_session() as session:
                        task = session.exec(select(EmailTasks).where(EmailTasks.message_id == message_id)).first()
                        if task:
                            task.status = "COMPLETED"
                            session.add(task)

                session.commit()

        # # 3. Background: Refresh watch every time or conditionally
        background_tasks.add_task(renew_gmail_watch)

        return Response(status_code=200)

    except Exception as e:
        print(f"Error processing webhook: {e}")
        traceback.print_exc()
        return Response(status_code=200)

@app.get("/health")
def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)