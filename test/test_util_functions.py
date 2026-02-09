

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
    
def test_extract_year_valid_date(util):
    """Test _extract_year returns correct year from date string"""
    assert util.extract_year("2022-01-15T00:00:00Z") == 2022
    assert util.extract_year("2020-12-31") == 2020

def test_extract_year_empty_string(util):
    """Test _extract_year returns 0 for empty string"""
    assert util.extract_year("") == 9999

def test_extract_year_none(util):
    """Test _extract_year returns 0 for None"""
    assert util.extract_year(None) == 9999

def test_extract_year_invalid_format(util):
    """Test _extract_year returns 0 for invalid format"""
    assert util.extract_year("invalid-date") == 9999