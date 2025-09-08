class TRANSFORMER:
    
    def transform_for_bar(records):
        data = {}
        for r in records:
            dept = r["department"]
            salary = r["salary"]
            data[dept] = data.get(dept, 0) + salary
        return [{"department": k, "total_salary": v} for k, v in data.items()]
    
    def transform_for_line(records):
        return [{"x": r["age"], "y": r["salary"], "name": r["name"]} for r in records]
    
    def transform_for_pie(records):
        data = {}
        for r in records:
            dept = r["department"]
            data[dept] = data.get(dept, 0) + 1
        return [{"label": k, "value": v} for k, v in data.items()]


