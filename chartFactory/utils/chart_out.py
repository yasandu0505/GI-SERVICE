from chartFactory.data_transformers.transformers_registry import TRANSFORMERS

def transform_data_for_chart(response, chart_type):
    columns = response["value"]["columns"]
    rows = response["value"]["rows"]
    records = [dict(zip(columns, row)) for row in rows]
    
    print(records)

    transformer = TRANSFORMERS.get(chart_type)
    if not transformer:
        raise ValueError(f"Unsupported chart type: {chart_type}")
    
    return transformer(records)
