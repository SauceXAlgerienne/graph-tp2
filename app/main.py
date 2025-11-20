from fastapi import FastAPI

app = FastAPI()


@app.get("/health")
def health():
    """
    Simple health endpoint used by the TP checks.
    Returns {"ok": true} when the API is running.
    """
    return {"ok": True}
