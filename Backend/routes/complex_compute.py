# complex_compute.py
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from services.ai_complexity_oracle import fetch_oracle_values, call_ibm_model as call_oracle_model
from services.ai_complexity_mssql import fetch_mssql_values, call_ibm_model as call_mssql_model
import logging

router = APIRouter(prefix="/migration_complexity", tags=["Data Migration Complexity Prediction"])

# Set up module-level logger
logger = logging.getLogger("complex_compute")
logging.basicConfig(level=logging.INFO)

@router.get("/fetch-oracle-db-values/{schema}")
def get_oracle_db_values(schema: str):
    """Fetch Oracle DB metrics."""
    try:
        result = fetch_oracle_values(schema)
        logger.info(f"Oracle metrics for schema={schema}: {result}")
        return result
    except HTTPException as e:
        logger.error(f"Oracle metrics error [{schema}]: {e.detail}")
        return JSONResponse(content={"error": str(e.detail)}, status_code=e.status_code)
    except Exception as e:
        logger.error(f"Oracle metrics exception [{schema}]: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

@router.get("/fetch-mssql-db-values/{schema}")
def get_mssql_db_values(schema: str):
    """Fetch MSSQL DB metrics."""
    try:
        result = fetch_mssql_values(schema)
        logger.info(f"MSSQL metrics for schema={schema}: {result}")
        return result
    except HTTPException as e:
        logger.error(f"MSSQL metrics error [{schema}]: {e.detail}")
        return JSONResponse(content={"error": str(e.detail)}, status_code=e.status_code)
    except Exception as e:
        logger.error(f"MSSQL metrics exception [{schema}]: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

@router.get("/predict-oracle-from-db")
def predict_oracle_from_db(schema: str):
    """Fetch Oracle DB metrics and get IBM ML prediction."""
    try:
        input_data = fetch_oracle_values(schema)
        logger.info(f"Predict Oracle: Input data for schema={schema}: {input_data}")

        prediction = call_oracle_model(input_data)
        logger.info(f"Predict Oracle: IBM ML raw response: {prediction}")

        label = ""
        probability = None

        preds = prediction.get("predictions", [])
        if preds and isinstance(preds, list) and "values" in preds[0]:
            values = preds[0]["values"]
            if values and isinstance(values, list) and len(values) > 0 and len(values[0]) > 0:
                label = values[0][0]          # "High Complexity" etc.
                if len(values[0]) > 1:
                    probability = values[0][1]  # probability array
        elif "prediction" in prediction:
            label = prediction["prediction"]

        if not label or (isinstance(label, str) and not label.strip()):
            logger.error(f"Predict Oracle: ML model returned empty prediction for schema={schema}. Model input: {input_data}, Model response: {prediction}")
            return JSONResponse(
                status_code=500,
                content={
                    "error": "ML model returned empty prediction label. Check input features and model deployment. See logs for request/response.",
                    "model_input": input_data,
                    "model_response": prediction
                }
            )

        return {"prediction": label, "probability": probability}

    except HTTPException as e:
        detail = e.detail if isinstance(e.detail, dict) else {"error": str(e.detail)}
        logger.error(f"Predict Oracle: HTTPException for schema={schema}: {detail}")
        return JSONResponse(content=detail, status_code=e.status_code)
    except Exception as e:
        logger.error(f"Predict Oracle: Exception for schema={schema}: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

@router.get("/predict-mssql-from-db")
def predict_mssql_from_db(schema: str):
    """Fetch MSSQL DB metrics and get IBM ML prediction."""
    try:
        input_data = fetch_mssql_values(schema)
        logger.info(f"Predict MSSQL: Input data for schema={schema}: {input_data}")

        prediction = call_mssql_model(input_data)
        logger.info(f"Predict MSSQL: IBM ML raw response: {prediction}")

        label = ""
        probability = None

        preds = prediction.get("predictions", [])
        if preds and isinstance(preds, list) and "values" in preds[0]:
            values = preds[0]["values"]
            if values and isinstance(values, list) and len(values) > 0 and len(values[0]) > 0:
                label = values[0][0]
                if len(values[0]) > 1:
                    probability = values[0][1]
        elif "prediction" in prediction:
            label = prediction["prediction"]

        if not label or (isinstance(label, str) and not label.strip()):
            logger.error(f"Predict MSSQL: ML model returned empty prediction for schema={schema}. Model input: {input_data}, Model response: {prediction}")
            return JSONResponse(
                status_code=500,
                content={
                    "error": "ML model returned empty prediction label. Check input features and model deployment. See logs for request/response.",
                    "model_input": input_data,
                    "model_response": prediction
                }
            )

        return {"prediction": label, "probability": probability}

    except HTTPException as e:
        detail = e.detail if isinstance(e.detail, dict) else {"error": str(e.detail)}
        logger.error(f"Predict MSSQL: HTTPException for schema={schema}: {detail}")
        return JSONResponse(content=detail, status_code=e.status_code)
    except Exception as e:
        logger.error(f"Predict MSSQL: Exception for schema={schema}: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)
