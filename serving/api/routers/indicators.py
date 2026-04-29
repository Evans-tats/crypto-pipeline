from fastapi import APIRouter, Request, HTTPException

router = APIRouter()

SUPPORTED_SYMBOLS = {"BTCUSDT", "ETHUSDT", "BNBUSDT"}

@router.get("/{symbol}")
async def get_indicator(symbol: str, request: Request):
    symbol = symbol.upper()
    if symbol not in SUPPORTED_SYMBOLS:
        raise HTTPException(status_code=400, detail=f"Unsupported symbol: {symbol}")
    redis = request.app.state.redis
    data = await redis.hgetall(f"indicators:{symbol}")
    if not data:
        raise HTTPException(status_code=503, detail=f"No indicator data found for {symbol}")
    return {
        "symbol": symbol,
        "indicator": data
    }

@router.get("/")
async def list_active_symbols(request: Request):
    redis = request.app.state.redis
    active = []
    for symbol in SUPPORTED_SYMBOLS:
        exists = await redis.exists(f"indicators:{symbol}")
        if exists:
            active.append(symbol)
    return {"active_symbols": active}