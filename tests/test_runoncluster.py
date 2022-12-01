from runoncluster import RunOnCluster
import pytest

@pytest.fixture(scope="session")
def rc():
    roc = RunOnCluster()
    return roc

# For testing settings types
@pytest.mark.parameterize("setting, type", [
    ("runname","Text"),
    ("is_archive","Binary"),
    ("n_images_per_measurement","Integer"),
    ("type_first","Binary"),
    ("measurements_in_archive","Integer"),
    ("max_walltime","Integer")
    ("account","Text"),
    ("partition","Text"),
    ("script_directory","Directory"),
    ("batch_mode","Binary"),
    ("revision","Integer"),
    ])

# For testing image grouping
@pytest.mark.parameterize("image_group, n_measurements, measurements_per_run, group_first, grouped_images", [
    ([1, 2, 3, 4, 5, 6, 7, 8], 4, 2, True, [(0,1),(0,2),(1,3),(1,4),(2,5),(2,6),(3,7),(3,8)]),
    ([1], 1, 1, True, [(0,1)]),
    ([1], 2, 1, True, [(0,1)]),
    ([1, 2, 3, 4, 5, 6, 7, 8], 2, 4, True, [(0,1),(0,2),(0,3),(0,4),(1,5),(1,6),(1,7),(1,8)]),
    ([1, 2, 3, 4], 4, 1, True, [(0,1),(1,2),(2,3),(3,4)]),
    ([1, 2, 3, 4], 8, 1, True, [(0,1),(2,2),(4,3),(6,4)]),
    ([], 1, 1, True, []),
    ([1, 2, 3, 4], 1, 8, True, [(0,1),(0,2),(0,3),(0,4)]),
    ([1, 2, 3, 4], 0, 0, True, [(0,1),(0,2),(0,3),(0,4)]),
])

# For testing script sanitisation
@pytest.mark.parameterize("scripts,cleaned_scripts", [
    ("some; bash; script;","some; bash; script;"),
    (";some ;bash ;script","some; bash; script;"),
    ("some bash script","some bash script;"),
    ("some -options ;; and -commands ;","some -options ; and -commands;"),
    (";some bash; script /r/n;","some bash; script /n;"),
    (";some; nightmarish -bash /r/n script ;; /n","some; nightmarish -bash /n script ; /n;"),
])

def test_instantiate(rc):
    assert rc is not None

def test_create_settings(rc,setting,type):
    # Test all settings are expected type.
    rc.create_settings()
    rc_props = vars(rc)
    assert type(rc_props[setting] == type)

def test_group_images(rc,image_group,n_measurements,measurements_per_run,groups_first,grouped_images):
    output = rc.group_images(image_group, n_measurements, measurements_per_run, groups_first)
    assert output == grouped_images

def test_sanitise_scripts(rc,scripts,cleaned_scripts):
    assert rc.sanitise_scripts(scripts) == cleaned_scripts