
import sys
import asyncio
from app.services.chat_service import ChatService

async def main():
    fast = False
    args = sys.argv[1:]
    if '--fast' in args:
        fast = True
        args.remove('--fast')
    if len(args) < 1:
        print("Usage: python generate_pipeline.py 'your message here' [--fast]")
        return
    message = args[0]
    chat_service = ChatService()
    result = await chat_service.process_message(message, fast=fast)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
