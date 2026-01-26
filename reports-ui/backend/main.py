from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import pymannkendall as mk

from data_loader import get_fields_from_elasticsearch, load_data_from_elasticsearch

ES_INDEX = "gkg"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/fields")
def get_fields():
    fields = get_fields_from_elasticsearch(ES_INDEX)
    return fields

@app.get("/trends")
def get_trends(
    start_date: str = Query(..., example="2025-04-01"),
    end_date: str = Query(..., example="2025-04-09"),
    trend_type: str = Query("increasing", enum=["increasing", "decreasing"]),
    top_n: int = Query(10, ge=1, le=50),
    entity_field: str = Query(..., example="V21AllNames.Name.keyword")
):
    df = load_data_from_elasticsearch(start_date, end_date, entity_field)

    pivot_df = df.pivot_table(index="date", columns="top_entity", values="count", fill_value=0)
    pivot_df.index = pd.to_datetime(pivot_df.index)

    trend_results = []
    for col in pivot_df.columns:
        result = mk.original_test(pivot_df[col])
        trend_results.append({
            "entity": col,
            "trend": result.trend,
            "p": result.p,
            "trend strength": result.Tau
        })

    trend_df = pd.DataFrame(trend_results)
    trend_df = trend_df[trend_df["p"] < 0.3]

    if trend_type == "increasing":
        trend_df = trend_df[trend_df["trend strength"] > 0].sort_values(by="trend strength", ascending=False)
    else:
        trend_df = trend_df[trend_df["trend strength"] < 0].sort_values(by="trend strength", ascending=True)

    return trend_df.head(top_n).to_dict(orient="records")

@app.get("/timeseries")
def get_timeseries(
    entities: str = "",  
    start_date: str = Query(..., example="2025-03-25"),
    end_date: str = Query(..., example="2025-04-09"),
    top_n: int = Query(10, ge=1, le=50),
    entity_field: str = Query(..., example="V21AllNames.Name.keyword")
):
    df = load_data_from_elasticsearch(start_date, end_date, entity_field)
    
    if df.empty:
        return {"error": "No data found for the given date range."}
    
    if not entities:
        trends = get_trends(start_date, end_date, "increasing", top_n, entity_field)
        entities = [trend["entity"] for trend in trends]
    else:
        entities = [e.strip() for e in entities.split(",") if e.strip()]

    df_subset = df[df['top_entity'].isin(entities)]
    if df_subset.empty:
        return {"error": "No data found for the specified entities."}

    df_pivot = df_subset.pivot_table(index="date", columns="top_entity", values="count", fill_value=0)
    if df_pivot.empty:
        return {"error": "No data found after pivoting."}

    df_pivot.reset_index(inplace=True)
    df_pivot["date"] = pd.to_datetime(df_pivot["date"]).dt.strftime("%Y-%m-%d")
    
    return df_pivot.to_dict(orient="records")
