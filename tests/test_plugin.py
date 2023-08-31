def test_hello(pytester):
    """Make sure that our plugin works."""

    # create a temporary conftest.py file
    pytester.makeconftest(
        """
        import pytest

        @pytest.fixture(params=[
            "Brianna",
            "Andreas",
            "Floris",
        ])
        def name(request):
            return request.param
    """
    )

    # create a temporary pytest test file
    pytester.makepyfile(
        """
        def test_hello_default(hello):
            assert hello() == "Hello World!"

        def test_hello_name(hello, name):
            assert hello(name) == "Hello {0}!".format(name)
    """
    )

    # run all tests with pytest
    result = pytester.runpytest()

    # check that all 4 tests passed
    result.assert_outcomes(passed=4)