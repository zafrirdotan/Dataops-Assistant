import sys
import asyncio
from app.services.chat_service import ChatService

async def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_pipeline.py 'your message here'")
        return
    message = sys.argv[1]
    chat_service = ChatService()
    result = await chat_service.process_message(message)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
