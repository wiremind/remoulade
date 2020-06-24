from remoulade.api.schema import PageSchema


class TestSchemeAPI:
    def test_valid_page_scheme(self):
        page = {
            "sort_column": "message_id",
            "sort_direction": "asc",
            "size": 100,
            "search_value": "12",
        }
        result = PageSchema().validate(page)
        assert len(result) == 0

    def test_invalid_page_scheme(self):
        page = {"sort_column": 1, "sort_direction": "up", "size": -10, "search_value": "12"}
        result = PageSchema().validate(page)
        assert result == {
            "sort_column": ["Not a valid string."],
            "size": ["Must be greater than or equal to 1 and less than or equal to 1000."],
            "sort_direction": ["Must be one of: asc, desc."],
        }
