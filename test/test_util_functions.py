

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

# Testing the new parameter in term function get_full_date
def test_term_success_with_get_full_date(util):
    start_date = "2022-07-26"
    end_date = "2024-09-23"

    result = util.term(start_date, end_date, get_full_date=True)

    assert result == "2022-07-26 - 2024-09-23"

def test_term_success_with_get_full_date_present(util):
    start_date = "2022-07-26"
    end_date = None

    result = util.term(start_date, end_date, get_full_date=True)

    assert result == "2022-07-26 - Present"

def test_term_success_with_empty_start_date_full_date(util):
    start_date = ""
    end_date = "2022-07-26"
    
    result = util.term(start_date, end_date, get_full_date=True)

    assert result == "Unknown"

def test_term_success_with_empty_both_dates_full_date(util):
    start_date = ""
    end_date = ""
    
    result = util.term(start_date, end_date, get_full_date=True)

    assert result == "Unknown"

# test extract year function
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

# test calculate match score function
def test_calculate_match_score_exact_match(util):
    """Test calculate_match_score returns 1.0 for exact match"""
    score = util.calculate_match_score("health", "health")
    assert score == 1.0

def test_calculate_match_score_starts_with(util):
    """Test calculate_match_score returns 0.8 for starts with match"""
    score = util.calculate_match_score("health", "Health Ministry")
    assert score == 0.8

def test_calculate_match_score_contains(util):
    """Test calculate_match_score returns 0.6 for contains match"""
    score = util.calculate_match_score("health", "Ministry of Health")
    assert score == 0.6

def test_calculate_match_score_no_match(util):
    """Test calculate_match_score returns 0.0 for no match"""
    score = util.calculate_match_score("health", "Education Department")
    assert score == 0.0

def test_calculate_match_score_empty_text(util):
    """Test calculate_match_score returns 0.0 for empty text"""
    score = util.calculate_match_score("health", "")
    assert score == 0.0

def test_calculate_match_score_none_text(util):
    """Test calculate_match_score returns 0.0 for None text"""
    score = util.calculate_match_score("health", None)
    assert score == 0.0
