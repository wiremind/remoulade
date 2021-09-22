from remoulade.api.state import StatesSchema


class TestSchemeAPI:
    def test_valid_page_scheme(self):
        page = {
            "sort_column": "message_id",
            "sort_direction": "asc",
            "size": 100,
            "offset": 0,
            "selected_actors": ["actor"],
            "selected_statuses": ["Success"],
            "selected_ids": ["id"],
            "start_datetime": "2021-08-16 10:00:00",
            "end_datetime": "2021-08-16 10:00:00",
        }
        result = StatesSchema().validate(page)
        assert len(result) == 0

    def test_invalid_page_scheme(self):
        page = {
            "sort_column": 1,
            "sort_direction": "up",
            "size": -10,
            "offset": "offset",
            "selected_actors": [1],
            "selected_statuses": ["status"],
            "selected_ids": [123],
            "start_datetime": 100000,
            "end_datetime": 100000,
        }
        result = StatesSchema().validate(page)
        assert result == {
            "end_datetime": ["Not a valid datetime."],
            "offset": ["Not a valid integer."],
            "selected_actors": {0: ["Not a valid string."]},
            "selected_ids": {0: ["Not a valid string."]},
            "selected_statuses": {0: ["Must be one of: Started, Pending, Skipped, Canceled, Failure, Success."]},
            "size": ["Must be greater than or equal to 1 and less than or equal to 1000."],
            "sort_column": ["Not a valid string."],
            "sort_direction": ["Must be one of: asc, desc."],
            "start_datetime": ["Not a valid datetime."],
        }
