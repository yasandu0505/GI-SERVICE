

# test term function
def test_term_success_with_end_date(util):
    start_date = "2022-07-26"
    end_date = "2024-09-23"

    result = util.term(start_date, end_date)

    assert result == "2022 Jul - 2024 Sep"

def test_term_success_with_empty_end_date(util):
    start_date = "2022-07-26"
    end_date = ""
    
    result = util.term(start_date,end_date)

    assert result == "2022 Jul - Present"

def test_term_success_with_empty_start_date(util):
    start_date = ""
    end_date = "2022-07-26"
    
    result = util.term(start_date,end_date)

    assert result == "Unknown"

def test_term_success_without_start_date(util):
    start_date = None
    end_date = ""
    
    result = util.term(start_date,end_date)

    assert result == "Unknown"

def test_term_success_without_end_date(util):
    start_date = "2022-07-26"
    end_date = None
    
    result = util.term(start_date,end_date)

    assert result == "2022 Jul - Present"

