import asyncio
from datetime import datetime, time, timedelta
from fastapi import Body, FastAPI,Response
from sqlmodel import select

from database import db_handler, EmailTasks
from utilities.gmail import GetHoldingsFromGmail

app = FastAPI()

async def run_daily_reauth():
    while True:
        # Calculate time until next 8 AM
        now = datetime.now()
        target = datetime.combine(now.date(), time(8, 0))
        if now >= target:
            target += timedelta(days=1)
        
        sleep_seconds = (target - now).total_seconds()
        print(f"Background reauth scheduled. Sleeping for {sleep_seconds} seconds until next run at {target}")
        await asyncio.sleep(sleep_seconds)
        
        print("Starting scheduled background reauth task...")
        try:
            gmail = GetHoldingsFromGmail()
            gmail.force_reauthenticate()
            print("Scheduled background reauth completed successfully.")
        except Exception as e:
            print(f"Scheduled background reauth failed: {e}")
 
@app.on_event("startup")
async def on_startup():
    try:
        db_handler.create_db_and_tables()
        print("Database initialized successfully.")
    except Exception as e:
        print(f"Database initialization failed: {e}")
        
    # Start the background task
    asyncio.create_task(run_daily_reauth())

@app.post("/process")
async def process_webhook(data: dict = Body(...)):
    # FastAPI automatically parses the JSON body into the 'data' variable
    message_id = data.get("messageId")

    if not message_id:
        return Response(content="Invalid request", status_code=400)

    print(f"Received direct request for message ID: {message_id}")
    
    with db_handler.get_session() as session:
        # Check if already processed
        task_exists = session.exec(select(EmailTasks).where(EmailTasks.message_id == message_id)).first()
        if task_exists and task_exists.status == "COMPLETED":
            print(f"Message ID {message_id} already processed.")
            return Response(status_code=200)

        if not task_exists:
            task = EmailTasks(message_id=message_id, status="PROCESSING") # type: ignore
            session.add(task)
            session.commit()

    # Process outside of the initial session to avoid keeping transaction open
    try:
        gmail = GetHoldingsFromGmail()
        ret = gmail.process_transactions(message_id)
        print(f"Processing result for message ID {message_id}: {ret}")
        status = "COMPLETED"
        status_code = 200
        response_content = "Processing completed successfully"
    except Exception as e:
        print(f"Failed to process message ID {message_id}: {e}")
        status = "FAILED"
        status_code = 500
        response_content = f"Failed to process message: {str(e)}"

    with db_handler.get_session() as session:
        task = session.exec(select(EmailTasks).where(EmailTasks.message_id == message_id)).first()
        if task:
            task.status = status
            session.add(task)
            session.commit()
            
    return Response(content=response_content, status_code=status_code)

@app.get("/health")
def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)