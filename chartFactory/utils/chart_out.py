from chartFactory.data_transformers.transformers_registry import TRANSFORMERS

def transform_data_for_chart(response, chart_type, x_axis, y_axis, label, value):
    columns = response["value"]["columns"]
    rows = response["value"]["rows"]
    records = [dict(zip(columns, row)) for row in rows]
    
    transformer = TRANSFORMERS.get(chart_type)
    if not transformer:
        raise ValueError(f"Unsupported chart type: {chart_type}")
    
    if chart_type == "line" or chart_type == "bar":
        return transformer(records, x_axis, y_axis, label)
    elif chart_type == "pie":
        return transformer(records, value, label)
    elif chart_type == "table":
        return transformer(records)
