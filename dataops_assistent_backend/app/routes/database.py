"""
Database routes for the DataOps Assistant API.
"""
from fastapi import APIRouter, HTTPException, Depends
from app.services.database_service import DatabaseService, get_database_service
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/test-connection")
async def test_database_connection(db_service: DatabaseService = Depends(get_database_service)):
    """Test database connection."""
    try:
        is_connected = await db_service.test_connection()
        return {
            "database_connected": is_connected,
            "message": "Database connection successful" if is_connected else "Database connection failed"
        }
    except Exception as e:
        logger.error(f"Database connection test error: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection test failed: {str(e)}")


@router.get("/pipelines")
async def get_pipelines(db_service: DatabaseService = Depends(get_database_service)):
    """Get all pipelines from the database."""
    try:
        pipelines = await db_service.fetch_all(
            "SELECT id, name, description, spec, code, status, created_at, updated_at FROM pipelines ORDER BY created_at DESC"
        )
        
        # Convert to list of dictionaries
        pipeline_list = []
        for pipeline in pipelines:
            pipeline_dict = {
                "id": pipeline[0],
                "name": pipeline[1],
                "description": pipeline[2],
                "spec": pipeline[3],
                "code": pipeline[4],
                "status": pipeline[5],
                "created_at": pipeline[6].isoformat() if pipeline[6] else None,
                "updated_at": pipeline[7].isoformat() if pipeline[7] else None
            }
            pipeline_list.append(pipeline_dict)
        
        return {
            "pipelines": pipeline_list,
            "count": len(pipeline_list)
        }
    except Exception as e:
        logger.error(f"Error fetching pipelines: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch pipelines: {str(e)}")


@router.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: int, db_service: DatabaseService = Depends(get_database_service)):
    """Get a specific pipeline by ID."""
    try:
        pipeline = await db_service.fetch_one_async(
            "SELECT * FROM pipelines WHERE id = :pipeline_id",
            {"pipeline_id": pipeline_id}
        )
        
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        pipeline_dict = {
            "id": pipeline[0],
            "name": pipeline[1],
            "description": pipeline[2],
            "spec": pipeline[3],
            "code": pipeline[4],
            "status": pipeline[5],
            "created_at": pipeline[6].isoformat() if pipeline[6] else None,
            "updated_at": pipeline[7].isoformat() if pipeline[7] else None
        }
        
        return {"pipeline": pipeline_dict}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching pipeline {pipeline_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch pipeline: {str(e)}")


@router.get("/chat-history")
async def get_chat_history(
    session_id: str = None, 
    limit: int = 50,
    db_service: DatabaseService = Depends(get_database_service)
):
    """Get chat history, optionally filtered by session ID."""
    try:
        if session_id:
            query = """
                SELECT * FROM chat_history 
                WHERE session_id = :session_id 
                ORDER BY created_at DESC 
                LIMIT :limit
            """
            params = {"session_id": session_id, "limit": limit}
        else:
            query = """
                SELECT * FROM chat_history 
                ORDER BY created_at DESC 
                LIMIT :limit
            """
            params = {"limit": limit}
        
        chat_messages = await db_service.fetch_all(query, params)
        
        # Convert to list of dictionaries
        chat_list = []
        for entry in chat_messages:
            chat_dict = {
                "id": entry[0],
                "session_id": entry[1],
                "user_message": entry[2],
                "assistant_response": entry[3],
                "created_at": entry[4].isoformat() if entry[4] else None
            }
            chat_list.append(chat_dict)
        
        return {
            "chat_history": chat_list,
            "count": len(chat_list),
            "session_id": session_id
        }
    except Exception as e:
        logger.error(f"Error fetching chat history: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch chat history: {str(e)}")


@router.get("/bank-transactions")
async def get_bank_transactions(
    limit: int = 100,
    offset: int = 0,
    user_id: int = None,
    merchant: str = None,
    category: str = None,
    status: str = None,
    db_service: DatabaseService = Depends(get_database_service)
):
    """Get bank transactions with optional filtering."""
    try:
        # Build WHERE clause based on filters
        where_conditions = []
        params = {"limit": limit, "offset": offset}
        
        if user_id:
            where_conditions.append("user_id = :user_id")
            params["user_id"] = user_id
        
        if merchant:
            where_conditions.append("merchant ILIKE :merchant")
            params["merchant"] = f"%{merchant}%"
        
        if category:
            where_conditions.append("category ILIKE :category")
            params["category"] = f"%{category}%"
        
        if status:
            where_conditions.append("status = :status")
            params["status"] = status
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        # Get transactions
        query = f"""
            SELECT 
                transaction_id, user_id, account_id, transaction_date, transaction_time,
                amount, currency, merchant, category, transaction_type, status,
                location, device, balance_after, notes, created_at
            FROM bank_transactions 
            WHERE {where_clause}
            ORDER BY transaction_date DESC, transaction_time DESC
            LIMIT :limit OFFSET :offset
        """
        
        transactions = await db_service.fetch_all(query, params)
        
        # Convert to list of dictionaries
        transaction_list = []
        for txn in transactions:
            transaction_dict = {
                "transaction_id": txn[0],
                "user_id": txn[1],
                "account_id": txn[2],
                "transaction_date": txn[3].isoformat() if txn[3] else None,
                "transaction_time": str(txn[4]) if txn[4] else None,
                "amount": float(txn[5]) if txn[5] else 0,
                "currency": txn[6],
                "merchant": txn[7],
                "category": txn[8],
                "transaction_type": txn[9],
                "status": txn[10],
                "location": txn[11],
                "device": txn[12],
                "balance_after": float(txn[13]) if txn[13] else 0,
                "notes": txn[14],
                "created_at": txn[15].isoformat() if txn[15] else None
            }
            transaction_list.append(transaction_dict)
        
        # Get total count for pagination
        count_query = f"SELECT COUNT(*) FROM bank_transactions WHERE {where_clause}"
        total_count = (await db_service.fetch_one_async(count_query, {k: v for k, v in params.items() if k not in ['limit', 'offset']}))[0]
        
        return {
            "transactions": transaction_list,
            "count": len(transaction_list),
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
            "filters": {
                "user_id": user_id,
                "merchant": merchant,
                "category": category,
                "status": status
            }
        }
    except Exception as e:
        logger.error(f"Error fetching bank transactions: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch bank transactions: {str(e)}")


@router.get("/bank-transactions/stats")
async def get_bank_transaction_stats(db_service: DatabaseService = Depends(get_database_service)):
    """Get bank transaction statistics."""
    try:
        stats_query = """
            SELECT 
                COUNT(*) as total_transactions,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT account_id) as unique_accounts,
                COUNT(DISTINCT merchant) as unique_merchants,
                MIN(transaction_date) as earliest_date,
                MAX(transaction_date) as latest_date,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_credits,
                SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as total_debits,
                COUNT(CASE WHEN status = 'Completed' THEN 1 END) as completed_transactions,
                COUNT(CASE WHEN status = 'Failed' THEN 1 END) as failed_transactions,
                COUNT(CASE WHEN status = 'Pending' THEN 1 END) as pending_transactions
            FROM bank_transactions
        """
        
        stats = await db_service.fetch_one_async(stats_query)
        
        # Get top merchants
        merchant_query = """
            SELECT merchant, COUNT(*) as transaction_count, SUM(ABS(amount)) as total_amount
            FROM bank_transactions 
            GROUP BY merchant 
            ORDER BY transaction_count DESC 
            LIMIT 10
        """
        top_merchants = await db_service.fetch_all(merchant_query)
        
        # Get category breakdown
        category_query = """
            SELECT category, COUNT(*) as transaction_count, SUM(ABS(amount)) as total_amount
            FROM bank_transactions 
            GROUP BY category 
            ORDER BY transaction_count DESC
        """
        categories = await db_service.fetch_all(category_query)
        
        return {
            "overall_stats": {
                "total_transactions": stats[0],
                "unique_users": stats[1],
                "unique_accounts": stats[2],
                "unique_merchants": stats[3],
                "earliest_date": stats[4].isoformat() if stats[4] else None,
                "latest_date": stats[5].isoformat() if stats[5] else None,
                "total_credits": float(stats[6]) if stats[6] else 0,
                "total_debits": float(stats[7]) if stats[7] else 0,
                "completed_transactions": stats[8],
                "failed_transactions": stats[9],
                "pending_transactions": stats[10]
            },
            "top_merchants": [
                {
                    "merchant": merchant[0],
                    "transaction_count": merchant[1],
                    "total_amount": float(merchant[2]) if merchant[2] else 0
                }
                for merchant in top_merchants
            ],
            "categories": [
                {
                    "category": cat[0],
                    "transaction_count": cat[1],
                    "total_amount": float(cat[2]) if cat[2] else 0
                }
                for cat in categories
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching bank transaction stats: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch bank transaction stats: {str(e)}")
