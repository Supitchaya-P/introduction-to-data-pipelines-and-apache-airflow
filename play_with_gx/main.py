import great_expectations as gx


def _validate_data():
    columns = ["NewConfirmed", "Date"]
    my_df = gx.read_csv("data.csv", names=columns)
    print(my_df)

    results = my_df.expect_column_values_to_be_between(
        column="NewConfirmed",
        min_value=0,
        max_value=322315,
    )
    print(results)

    element_count = results["result"]["element_count"]
    unexpected_count = results["result"]["unexpected_count"]
    expected_count = element_count - unexpected_count
    validity = expected_count / element_count * 100
    print(validity)

    assert results["success"] is True


_validate_data()