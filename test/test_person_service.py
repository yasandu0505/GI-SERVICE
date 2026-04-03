import pytest
from unittest.mock import AsyncMock, patch
from src.models.organisation_schemas import Entity, Relation
from src.models.person_schemas import PersonResponse
from src.exception.exceptions import BadRequestError, InternalServerError, NotFoundError
from datetime import date
from src.enums import KindMinorEnum

# --- Tests for is_president_during ---

@pytest.mark.asyncio
async def test_is_president_during_true(person_service):
    ministry_start = "2020-01-01T00:00:00Z"
    ministry_end = "2021-01-01T00:00:00Z"
    president_relations = [
        Relation(startTime="2019-01-01T00:00:00Z", endTime="2020-06-01T00:00:00Z")
    ]
    result = person_service.is_president_during(
        president_relations, ministry_start, ministry_end
    )
    assert result is True


@pytest.mark.asyncio
async def test_is_president_during_false(person_service):
    ministry_start = "2020-01-01T00:00:00Z"
    ministry_end = "2021-01-01T00:00:00Z"
    president_relations = [
        Relation(startTime="2022-01-01T00:00:00Z", endTime="2023-01-01T00:00:00Z")
    ]
    result = person_service.is_president_during(
        president_relations, ministry_start, ministry_end
    )
    assert result is False


@pytest.mark.asyncio
async def test_is_president_during_ongoing_presidency(person_service):
    ministry_start = "2020-01-01T00:00:00Z"
    ministry_end = "2021-01-01T00:00:00Z"
    president_relations = [
        Relation(startTime="2020-06-01T00:00:00Z", endTime="")
    ]  # Ongoing is empty string
    result = person_service.is_president_during(
        president_relations, ministry_start, ministry_end
    )
    assert result is True


@pytest.mark.asyncio
async def test_is_president_during_ongoing_ministry(person_service):
    ministry_start = "2020-01-01T00:00:00Z"
    ministry_end = ""  # Ongoing ministry
    president_relations = [
        Relation(startTime="2020-06-01T00:00:00Z", endTime="2020-12-01T00:00:00Z")
    ]
    result = person_service.is_president_during(
        president_relations, ministry_start, ministry_end
    )
    assert result is True


@pytest.mark.asyncio
async def test_is_president_during_exact_touch(person_service):
    ministry_start = "2020-01-01T00:00:00Z"
    ministry_end = "2020-06-01T00:00:00Z"
    president_relations = [
        Relation(startTime="2020-06-01T00:00:00Z", endTime="2021-01-01T00:00:00Z")
    ]
    # ministry ends same day presidency starts
    result = person_service.is_president_during(
        president_relations, ministry_start, ministry_end
    )
    assert result is False


# --- Tests for fetch_person_history ---


@pytest.mark.asyncio
async def test_fetch_person_history_success(person_service, mock_opengin_service):
    person_id = "person_123"
    mock_opengin_service.fetch_relation.side_effect = [
        [
            Relation(
                relatedEntityId="min_1",
                startTime="2020-01-01T00:00:00Z",
                endTime="2021-01-01T00:00:00Z",
            )
        ],
        [],  # No president relations
    ]
    with patch.object(
        person_service, "enrich_history_item", new_callable=AsyncMock
    ) as mock_enrich:
        mock_enrich.return_value = {
            "id": "min_1",
            "name": "Min",
            "term": "T",
            "is_president": False,
            "start_time": "2020",
            "end_time": "2021",
        }
        result = await person_service.fetch_person_history(person_id)
        assert result["ministries_worked_at"] == 1
        assert len(result["ministry_history"]) == 1


@pytest.mark.asyncio
async def test_fetch_person_history_sorting(person_service, mock_opengin_service):
    person_id = "person_123"
    mock_opengin_service.fetch_relation.side_effect = [
        [
            Relation(
                relatedEntityId="old",
                startTime="2010-01-01T00:00:00Z",
                endTime="2012-01-01T00:00:00Z",
            ),
            Relation(
                relatedEntityId="ongoing", startTime="2022-01-01T00:00:00Z", endTime=""
            ),
            Relation(
                relatedEntityId="recent",
                startTime="2020-01-01T00:00:00Z",
                endTime="2021-01-01T00:00:00Z",
            ),
        ],
        [],
    ]

    # We need to mock enrich_history_item to return items with different end times
    async def side_effect(rel, pres_rels):
        return {
            "id": rel.relatedEntityId,
            "start_time": rel.startTime,
            "end_time": rel.endTime,
        }

    with patch.object(person_service, "enrich_history_item", side_effect=side_effect):
        result = await person_service.fetch_person_history(person_id)
        history = result["ministry_history"]

        # Expected order: ongoing (""), then 2021, then 2012
        assert history[0]["id"] == "ongoing"
        assert history[1]["id"] == "recent"
        assert history[2]["id"] == "old"


@pytest.mark.asyncio
async def test_fetch_person_history_no_ministries(person_service, mock_opengin_service):
    person_id = "person_123"
    mock_opengin_service.fetch_relation.return_value = []
    result = await person_service.fetch_person_history(person_id)
    assert result["ministries_worked_at"] == 0
    assert result["ministry_history"] == []


@pytest.mark.asyncio
async def test_fetch_person_history_internal_error(
    person_service, mock_opengin_service
):
    person_id = "person_123"
    with patch(
        "src.services.person_service.asyncio.gather",
        side_effect=Exception("Gather fail"),
    ):
        with pytest.raises(InternalServerError):
            await person_service.fetch_person_history(person_id)


@pytest.mark.asyncio
async def test_fetch_person_history_bad_request(person_service):
    with pytest.raises(BadRequestError):
        await person_service.fetch_person_history("")


# --- Tests for enrich_history_item ---


@pytest.mark.asyncio
async def test_enrich_history_item_success(person_service, mock_opengin_service):
    person_id = "person_123"
    relation = Relation(
        relatedEntityId="min_1",
        startTime="2020-01-01T00:00:00Z",
        endTime="2021-01-01T00:00:00Z",
    )

    mock_opengin_service.get_entities.return_value = [
        Entity(id="min_1", name="raw_name")
    ]

    with (
        patch(
            "src.services.person_service.Util.decode_protobuf_attribute_name",
            return_value="Ministry of Magic",
        ),
        patch(
            "src.services.person_service.Util.term",
            return_value="2020-01-01 - 2021-01-01",
        ),
    ):
        result = await person_service.enrich_history_item(relation, [])
        assert result["id"] == "min_1"
        assert result["name"] == "Ministry of Magic"
        assert result["is_president"] is False


@pytest.mark.asyncio
async def test_enrich_history_item_not_found(person_service, mock_opengin_service):
    person_id = "person_123"
    relation = Relation(
        relatedEntityId="ghost",
        startTime="2020-01-01T00:00:00Z",
        endTime="2021-01-01T00:00:00Z",
    )

    mock_opengin_service.get_entities.return_value = []  # Not found

    result = await person_service.enrich_history_item(relation, [])
    assert result is None


@pytest.mark.asyncio
async def test_enrich_history_item_error(person_service, mock_opengin_service):
    person_id = "person_123"
    relation = Relation(
        relatedEntityId="min_1",
        startTime="2020-01-01T00:00:00Z",
        endTime="2021-01-01T00:00:00Z",
    )

    mock_opengin_service.get_entities.side_effect = Exception("Timeout")

    result = await person_service.enrich_history_item(relation, [])
    assert result is None  # Service returns None on error within enrichment


# --- Tests for fetch_person_profile ---


@pytest.mark.asyncio
async def test_fetch_person_profile_success(person_service, mock_opengin_service):
    person_id = "person_123"

    mock_opengin_service.get_entities.return_value = None
    mock_opengin_service.get_attributes.return_value = {
        "start": "",
        "end": "",
        "value": "encoded",
    }

    fake_json = {
        "type": KindMinorEnum.TABULAR.value,
        "data": {
            "columns": [
                "name",
                "political_party",
                "date_of_birth",
                "religion",
                "profession",
                "email",
                "phone_number",
                "education_qualifications",
                "professional_qualifications",
                "image_url",
            ],
            "rows": [
                [
                    "test name",
                    "test party",
                    "1971-10-19",
                    "test religion",
                    "test profession",
                    "test@test.com",
                    "0000000000",
                    "test education",
                    "test professional qualification",
                    "test image url",
                ]
            ],
        },
    }

    with (
        patch(
            "src.services.person_service.Util.transform_data_for_chart",
            return_value=fake_json,
        ),
        patch(
            "src.services.person_service.Util.calculate_age",
            return_value=54,
        ),
    ):
        result = await person_service.fetch_person_profile(person_id)

    assert isinstance(result, PersonResponse)

    assert result.name == "test name"
    assert result.political_party == "test party"
    assert result.date_of_birth == date(1971, 10, 19)
    assert result.age == 54

    mock_opengin_service.get_entities.assert_called_once()
    mock_opengin_service.get_attributes.assert_called_once_with(
        category_id=person_id,
        dataset_name=f"{person_id}_profile",
    )


@pytest.mark.asyncio
async def test_fetch_person_profile_invalid_id(person_service):
    with pytest.raises(BadRequestError):
        await person_service.fetch_person_profile(" ")


@pytest.mark.asyncio
async def test_fetch_person_profile_rows_empty_should_raise_not_found(
    person_service, mock_opengin_service
):
    person_id = "person_123"

    mock_opengin_service.get_entities.return_value = None
    mock_opengin_service.get_attributes.return_value = {
        "start": "",
        "end": "",
        "value": "encoded",
    }

    with (
        patch(
            "src.services.person_service.Util.transform_data_for_chart",
            return_value={
                "data": {
                    "columns": [
                        "name",
                        "political_party",
                        "date_of_birth",
                        "religion",
                        "profession",
                        "email",
                        "phone_number",
                        "education_qualifications",
                        "professional_qualifications",
                        "image_url",
                    ],
                    "rows": [],
                }
            },
        ),
        patch("src.services.person_service.Util.calculate_age") as mock_calculate_age,
    ):
        with pytest.raises(NotFoundError) as exc:
            await person_service.fetch_person_profile(person_id)

        assert f"Profile data not found for person {person_id}" in str(exc.value)

        mock_calculate_age.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_person_profile_with_null_values_coming_from_upstream(
    person_service, mock_opengin_service
):
    person_id = "person_123"

    mock_opengin_service.get_entities.return_value = None
    mock_opengin_service.get_attributes.return_value = {
        "start": "",
        "end": "",
        "value": "encoded",
    }

    with patch(
        "src.services.person_service.Util.transform_data_for_chart",
        return_value={
            "data": {
                "columns": [
                    "name",
                    "political_party",
                    "date_of_birth",
                    "religion",
                    "profession",
                    "email",
                    "phone_number",
                    "education_qualifications",
                    "professional_qualifications",
                    "image_url",
                ],
                "rows": [
                    [
                        "Test Name",
                        None,
                        "1990-01-01",
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    ]
                ],
            }
        },
    ):
        result = await person_service.fetch_person_profile(person_id)

        assert result.name == "Test Name"
        assert result.political_party is None
        assert result.date_of_birth == date(1990, 1, 1)
        assert result.religion is None
        assert result.profession is None
        assert result.email is None
        assert result.phone_number is None
        assert result.education_qualifications is None
        assert result.professional_qualifications is None
        assert result.image_url is None
        assert result.age == 36

@pytest.mark.asyncio
async def test_fetch_person_profile_with_null_values_coming_from_upstream_2(
    person_service, mock_opengin_service
):
    person_id = "person_123"

    mock_opengin_service.get_entities.return_value = None
    mock_opengin_service.get_attributes.return_value = {
        "start": "",
        "end": "",
        "value": "encoded",
    }

    with patch(
        "src.services.person_service.Util.transform_data_for_chart",
        return_value={
            "data": {
                "columns": [
                    "name",
                    "political_party",
                    "date_of_birth",
                    "religion",
                    "profession",
                    "email",
                    "phone_number",
                    "education_qualifications",
                    "professional_qualifications",
                    "image_url",
                ],
                "rows": [
                    [
                        "Test Name",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                    ]
                ],
            }
        },
    ):
        result = await person_service.fetch_person_profile(person_id)

        assert result.name == "Test Name"
        assert result.political_party == ""
        assert result.date_of_birth is None
        assert result.religion == ""
        assert result.profession == ""
        assert result.email == ""
        assert result.phone_number == ""
        assert result.education_qualifications == ""
        assert result.professional_qualifications == ""
        assert result.image_url == ""
        assert result.age is None


@pytest.mark.asyncio
async def test_fetch_person_profile_with_null_values_coming_from_upstream_3(
    person_service, mock_opengin_service
):
    person_id = "person_123"

    mock_opengin_service.get_entities.return_value = None
    mock_opengin_service.get_attributes.return_value = {
        "start": "",
        "end": "",
        "value": "encoded",
    }

    with patch(
        "src.services.person_service.Util.transform_data_for_chart",
        return_value={
            "data": {
                "columns": [
                    "name",
                    "political_party",
                    "date_of_birth",
                    "religion",
                    "profession",
                    "email",
                    "phone_number",
                    "education_qualifications",
                    "professional_qualifications",
                    "image_url",
                ],
                "rows": [
                    [
                        "Test Name",
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    ]
                ],
            }
        },
    ):
        result = await person_service.fetch_person_profile(person_id)

        assert result.name == "Test Name"
        assert result.political_party is None
        assert result.date_of_birth is None
        assert result.religion is None
        assert result.profession is None
        assert result.email is None
        assert result.phone_number is None
        assert result.education_qualifications is None
        assert result.professional_qualifications is None
        assert result.image_url is None
        assert result.age is None

@pytest.mark.asyncio
async def test_fetch_person_profile_with_null_name_should_raise_error(
    person_service, mock_opengin_service
):
    person_id = "person_123"

    mock_opengin_service.get_entities.return_value = None
    mock_opengin_service.get_attributes.return_value = {}

    with patch(
        "src.services.person_service.Util.transform_data_for_chart",
        return_value={
            "data": {
                "columns": [
                    "name",
                    "political_party",
                    "date_of_birth",
                    "religion",
                    "profession",
                    "email",
                    "phone_number",
                    "education_qualifications",
                    "professional_qualifications",
                    "image_url",
                ],
                "rows": [
                    [
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    ]
                ],
            }
        },
    ):
        with pytest.raises(InternalServerError):
            await person_service.fetch_person_profile(person_id)


@pytest.mark.asyncio
async def test_fetch_person_profile_internal_error(
    person_service, mock_opengin_service
):
    mock_opengin_service.get_entities.return_value = None
    mock_opengin_service.get_attributes.side_effect = Exception("boom")

    with pytest.raises(InternalServerError):
        await person_service.fetch_person_profile("person_123")

# --- Tests for fetch_all_presidents ---

@pytest.mark.asyncio
async def test_fetch_all_presidents_success(person_service, mock_opengin_service):

    mock_opengin_service.fetch_relation.return_value = [
        Relation(relatedEntityId="p1", startTime="2020-01-01T00:00:00Z", endTime="2022-01-01T00:00:00Z"),
        Relation(relatedEntityId="p1", startTime="2022-06-01T00:00:00Z", endTime="")
    ]

    mock_opengin_service.get_entities.side_effect = [
        [Entity(id="g_org", created="2020-05-01T00:00:00Z", name="org_gzt")],
        [Entity(id="g_per", created="2022-08-01T00:00:00Z", name="per_gzt")],
        [Entity(id="p1", name="President One")] # president name fetch
    ]

    with patch("src.services.person_service.Util.decode_protobuf_attribute_name", side_effect=lambda x: x):
        result = await person_service.fetch_all_presidents()

        presidents = result["presidents"]
        assert len(presidents) == 1
        president = presidents[0]
        assert president["id"] == "p1"
        assert president["name"] == "President One"
        assert len(president["terms"]) == 2
        
        # Check gazettes are inside the first term (2020 term)
        term1_gazettes = president["terms"][0]["gazettes_published"]
        assert len(term1_gazettes) == 1
        assert term1_gazettes[0]["date"] == "2020-05-01"
        assert "org_gzt" in term1_gazettes[0]["ids"]

        # Check gazettes are inside the second term (2022 term)
        term2_gazettes = president["terms"][1]["gazettes_published"]
        assert len(term2_gazettes) == 1
        assert term2_gazettes[0]["date"] == "2022-08-01"
        assert "per_gzt" in term2_gazettes[0]["ids"]


@pytest.mark.asyncio
async def test_fetch_all_presidents_no_data(person_service, mock_opengin_service):
    mock_opengin_service.fetch_relation.return_value = []
    
    result = await person_service.fetch_all_presidents()
    
    assert result == {"presidents": []}

@pytest.mark.asyncio
async def test_fetch_all_presidents_no_gazettes(person_service, mock_opengin_service):
    mock_opengin_service.fetch_relation.return_value = [
        Relation(relatedEntityId="p1", startTime="2020-01-01T00:00:00Z", endTime="")
    ]

    mock_opengin_service.get_entities.side_effect = [
        [],  # No organization gazettes
        [],  # No person gazettes
        [Entity(id="p1", name="President One")] 
    ]

    with patch("src.services.person_service.Util.decode_protobuf_attribute_name", side_effect=lambda x: x):
        result = await person_service.fetch_all_presidents()

        presidents = result["presidents"]
        assert len(presidents) == 1
        assert presidents[0]["name"] == "President One"
        assert presidents[0]["terms"][0]["gazettes_published"] == []


@pytest.mark.asyncio
async def test_fetch_all_presidents_sorting_with_multiple_terms(person_service, mock_opengin_service):
    # Setup: 
    # p_old started in 2010
    # p_multi started in 2005 AND 2022. 
    # Even though p_multi has a 2005 term, their 2022 term should put them at the TOP.

    mock_opengin_service.fetch_relation.return_value = [
        Relation(relatedEntityId="p_old", startTime="2010-01-01T00:00:00Z", endTime="2015-01-01T00:00:00Z"),
        Relation(relatedEntityId="p_multi", startTime="2005-01-01T00:00:00Z", endTime="2009-12-31T00:00:00Z"),
        Relation(relatedEntityId="p_multi", startTime="2022-01-01T00:00:00Z", endTime="")
    ]

    mock_opengin_service.get_entities.side_effect = [
        [], [], # no gazettes for either
        [Entity(id="p_old", name="Old President")],
        [Entity(id="p_multi", name="Multi-term President")]
    ]

    with patch("src.services.person_service.Util.decode_protobuf_attribute_name", side_effect=lambda x: x):
        result = await person_service.fetch_all_presidents()

        presidents = result["presidents"]
        
        # p_multi should be first because 2022 > 2010
        assert presidents[0]["id"] == "p_multi"
        assert presidents[1]["id"] == "p_old"


@pytest.mark.asyncio
async def test_fetch_all_presidents_internal_error(person_service, mock_opengin_service):
    mock_opengin_service.fetch_relation.side_effect = Exception("Database down")
    
    with pytest.raises(InternalServerError):
        await person_service.fetch_all_presidents()


