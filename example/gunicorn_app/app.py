import asyncio
import logging
from fastapi import FastAPI
from pyxxl import job_hander

logger = logging.getLogger("pyxxl")
handler = logging.StreamHandler()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@job_hander
async def test_task():
    await asyncio.sleep(10)
    return "成功10"
