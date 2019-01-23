from runoncluster import RunOnCluster

rc = RunOnCluster()

def test_instantiate():
    assert rc is not None

def test_create_settings():
    rc.create_settings()

def test_settings():
    rc.settings()

def test_visible_settings():
    rc.visible_settings()

def test_help_settings():
    rc.help_settings()

def test_group_images():
    imagelist = [1,2,3,4,5,6,7,8]
    output = rc.group_images(imagelist, 4, 2, groups_first = False)
    assert output == [(0,1),(0,2),(1,3),(1,4),(0,5),(0,6),(1,7),(1,8)]

    output = rc.group_images(imagelist, 4, 2, groups_first = True)
    assert output == [(0,1),(0,2),(0,3),(0,4),(1,5),(1,6),(1,7),(1,8)]
