from fastapi import Body, FastAPI,Response
from sqlmodel import select

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
    gmail = GetHoldingsFromGmail()
    ret = gmail.process_transactions(message_id)
    print(f"Processing result for message ID {message_id}: {ret}")

    with db_handler.get_session() as session:
        task = session.exec(select(EmailTasks).where(EmailTasks.message_id == message_id)).first()
        if task:
            task.status = "COMPLETED"
            session.add(task)
            session.commit()
    return Response(status_code=200)

@app.get("/health")
def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)