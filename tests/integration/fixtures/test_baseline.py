def test_listing_workspaces(acc):
    workspaces = acc.workspaces.list()
    assert len(workspaces) >= 1
