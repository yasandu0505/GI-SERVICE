class TRANSFORMER:
    
    @staticmethod
    def parse_response(response):
        """
        Parse the response and convert it to a list of dictionaries
        """
        # Handle different response formats
        if isinstance(response, dict) and 'value' in response:
            data = response['value']
        else:
            data = response
        
        # Case 1: Data is already a list of dictionaries
        if isinstance(data, list) and data and isinstance(data[0], dict):
            records = data
            columns = list(data[0].keys()) if data else []
            return records, columns
        
        # Case 2: Data has 'columns' and 'rows' structure
        if isinstance(data, dict):
            columns = data.get('columns', [])
            rows = data.get('rows', [])
            
            # Convert rows to list of dictionaries
            records = []
            for row in rows:
                record = {}
                for i, column in enumerate(columns):
                    if i < len(row):
                        record[column] = row[i]
                records.append(record)
            
            return records, columns
        
        # Case 3: Data is just a list of lists (rows without column names)
        if isinstance(data, list) and data and isinstance(data[0], list):
            # Generate generic column names
            columns = [f"col_{i}" for i in range(len(data[0]))]
            records = []
            for row in data:
                record = {}
                for i, column in enumerate(columns):
                    if i < len(row):
                        record[column] = row[i]
                records.append(record)
            
            return records, columns
        
        # Case 4: Unknown format - return empty
        return [], []
    
    @staticmethod
    def get_numeric_columns(records):
        """
        Identify numeric columns from the records
        """
        if not records:
            return []
        
        numeric_cols = []
        sample_record = records[0]
        
        for col, value in sample_record.items():
            if isinstance(value, (int, float)):
                numeric_cols.append(col)
        
        return numeric_cols
    
    @staticmethod
    def get_categorical_columns(records):
        """
        Identify categorical (string) columns from the records
        """
        if not records:
            return []
        
        categorical_cols = []
        sample_record = records[0]
        
        for col, value in sample_record.items():
            if isinstance(value, str):
                categorical_cols.append(col)
        
        return categorical_cols
    
    

    @staticmethod
    def transform_for_line(response, x_col=None, y_col=None, label_col=None, chart_type="line"):
        """
        Transform data for line chart
    
        Args:
            response: The response data
            x_col: Column to use for x-axis (auto-detected if None)
            y_col: Column to use for y-axis (auto-detected if None)  
            label_col: Column to use for point labels (optional)
            chart_type: Type of chart (default: "line")
    
        Returns:
        dict: Chart-ready JSON
        """
        records, columns = TRANSFORMER.parse_response(response)

        if not records:
            return {
                "chartType": chart_type,
                "xAxis": x_col or "",
                "yAxis": y_col or "",
                "data": []
            }
    
        numeric_cols = TRANSFORMER.get_numeric_columns(records)
        categorical_cols = TRANSFORMER.get_categorical_columns(records)
    
        # Auto-detect columns if not specified
        if not x_col:
            x_col = numeric_cols[0] if len(numeric_cols) > 1 else (numeric_cols[0] if numeric_cols else columns[0])
    
        if not y_col:
            # Pick a numeric column different from x_col
            y_candidates = [col for col in numeric_cols if col != x_col]
            y_col = y_candidates[0] if y_candidates else (numeric_cols[0] if numeric_cols else (columns[1] if len(columns) > 1 else columns[0]))
    
        if not label_col:
            # Use first categorical column or fallback
            label_col = categorical_cols[0] if categorical_cols else columns[0]
    
        # Build chart points
        data_points = []
        for record in records:
            point = {
                "x": record.get(x_col, 0),
                "y": record.get(y_col, 0)
            }
            if label_col and label_col in record:
                point["label"] = record[label_col]
            data_points.append(point)
    
        # Wrap result in chart config
        return {
            "chartType": chart_type,
            "xAxis": x_col,
            "yAxis": y_col,
            "data": data_points
        }

