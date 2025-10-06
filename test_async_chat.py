#!/usr/bin/env python3
"""
Test script to validate async chat endpoint can handle multiple concurrent calls
"""
import asyncio
import aiohttp
import time
import json

async def send_chat_request(session, request_id, message):
    """Send a single chat request"""
    url = "http://localhost:8000/chat"
    payload = {"message": message}
    headers = {"Content-Type": "application/json"}
    
    start_time = time.time()
    try:
        async with session.post(url, json=payload, headers=headers) as response:
            end_time = time.time()
            duration = end_time - start_time
            
            if response.status == 200:
                data = await response.json()
                print(f"Request {request_id}: SUCCESS in {duration:.2f}s")
                return {"id": request_id, "status": "success", "duration": duration, "response": data}
            else:
                error_text = await response.text()
                print(f"Request {request_id}: FAILED in {duration:.2f}s - {response.status}: {error_text}")
                return {"id": request_id, "status": "failed", "duration": duration, "error": error_text}
                
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        print(f"Request {request_id}: EXCEPTION in {duration:.2f}s - {str(e)}")
        return {"id": request_id, "status": "exception", "duration": duration, "error": str(e)}

async def test_concurrent_requests():
    """Test multiple concurrent chat requests"""
    print("Testing async chat endpoint with concurrent requests...")
    print("=" * 50)
    
    # Create different test messages
    messages = [
        "Create a pipeline to load CSV data from bank_transactions.csv and validate transaction_id field",
        "Build a data pipeline to process bank transaction data with data cleaning",
        "Generate ETL pipeline for financial data validation and transformation",
        "Create pipeline for loading and processing transaction data from CSV",
        "Build data processing pipeline with PostgreSQL destination"
    ]
    
    start_time = time.time()
    
    async with aiohttp.ClientSession() as session:
        # Create tasks for concurrent execution
        tasks = []
        for i, message in enumerate(messages, 1):
            task = send_chat_request(session, i, message)
            tasks.append(task)
        
        # Execute all requests concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    end_time = time.time()
    total_duration = end_time - start_time
    
    print("\n" + "=" * 50)
    print("RESULTS SUMMARY:")
    print(f"Total time for {len(messages)} concurrent requests: {total_duration:.2f}s")
    
    successful = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
    failed = sum(1 for r in results if isinstance(r, dict) and r.get("status") in ["failed", "exception"])
    exceptions = sum(1 for r in results if not isinstance(r, dict))
    
    print(f"Successful requests: {successful}")
    print(f"Failed requests: {failed}")
    print(f"Exceptions: {exceptions}")
    
    if successful > 0:
        avg_duration = sum(r["duration"] for r in results if isinstance(r, dict) and r.get("status") == "success") / successful
        print(f"Average response time: {avg_duration:.2f}s")
    
    print("\nDetailed results:")
    for result in results:
        if isinstance(result, dict):
            print(f"  Request {result['id']}: {result['status'].upper()} ({result['duration']:.2f}s)")
        else:
            print(f"  Exception: {result}")

async def test_sequential_requests():
    """Test sequential requests for comparison"""
    print("\nTesting sequential requests for comparison...")
    print("=" * 50)
    
    messages = [
        "Create a simple pipeline to load CSV data",
        "Build a basic data pipeline"
    ]
    
    start_time = time.time()
    
    async with aiohttp.ClientSession() as session:
        results = []
        for i, message in enumerate(messages, 1):
            result = await send_chat_request(session, i, message)
            results.append(result)
    
    end_time = time.time()
    total_duration = end_time - start_time
    
    print(f"Total time for {len(messages)} sequential requests: {total_duration:.2f}s")

async def main():
    """Main test function"""
    print("🚀 Starting Async Chat Endpoint Test")
    print("Testing if the chat route can handle multiple concurrent calls")
    print()
    
    # Test concurrent requests
    await test_concurrent_requests()
    
    # Test sequential requests for comparison
    await test_sequential_requests()
    
    print("\n✅ Test completed!")

if __name__ == "__main__":
    asyncio.run(main())
