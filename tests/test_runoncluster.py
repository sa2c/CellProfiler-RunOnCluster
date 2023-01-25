import pytest

from ..runoncluster import RunOnCluster


@pytest.fixture(scope="session")
def rc():
    roc = RunOnCluster()
    return roc


def test_instantiate(rc):
    assert rc is not None


# For testing settings types
@pytest.mark.parametrize(
    "setting, obj_type",
    [
        ("runname", "Text"),
        ("is_archive", "Binary"),
        ("n_images_per_measurement", "Integer"),
        ("type_first", "Binary"),
        ("measurements_in_archive", "Integer"),
        ("max_walltime", "Integer"),
        ("account", "Text"),
        ("partition", "Text"),
        ("script_directory", "Directory"),
        ("batch_mode", "Binary"),
        ("revision", "Integer"),
    ],
)
def test_create_settings(rc, setting, obj_type):
    # Test all settings are expected type.
    rc.create_settings()
    rc_props = vars(rc)
    assert type(rc_props.get(setting)).__name__ == obj_type


# For testing image grouping
@pytest.mark.parametrize(
    "image_group, n_measurements, measurements_per_run, groups_first, grouped_images",
    [
        (
            ["a", "b", "c", "d", "e", "f", "g", "h"],
            1,
            1,
            True,
            [
                (0, "a"),
                (0, "b"),
                (0, "c"),
                (0, "d"),
                (0, "e"),
                (0, "f"),
                (0, "g"),
                (0, "h"),
            ],
        ),
        (
            ["a", "b", "c", "d", "e", "f", "g", "h"],
            2,
            1,
            True,
            [
                (0, "a"),
                (0, "b"),
                (0, "c"),
                (0, "d"),
                (1, "e"),
                (1, "f"),
                (1, "g"),
                (1, "h"),
            ],
        ),
        (
            ["a", "b", "c", "d", "e", "f", "g", "h"],
            4,
            1,
            True,
            [
                (0, "a"),
                (0, "b"),
                (1, "c"),
                (1, "d"),
                (2, "e"),
                (2, "f"),
                (3, "g"),
                (3, "h"),
            ],
        ),
        (
            ["a", "b", "c", "d", "e", "f", "g", "h"],
            4,
            1,
            False,
            [
                (0, "a"),
                (1, "b"),
                (2, "c"),
                (3, "d"),
                (0, "e"),
                (1, "f"),
                (2, "g"),
                (3, "h"),
            ],
        ),
        (
            ["a", "b", "c", "d", "e", "f", "g", "h"],
            4,
            2,
            True,
            [
                (0, "a"),
                (0, "b"),
                (0, "c"),
                (0, "d"),
                (1, "e"),
                (1, "f"),
                (1, "g"),
                (1, "h"),
            ],
        ),
        (
            ["a", "b", "c", "d", "e", "f", "g", "h"],
            4,
            2,
            False,
            [
                (0, "a"),
                (0, "b"),
                (1, "c"),
                (1, "d"),
                (0, "e"),
                (0, "f"),
                (1, "g"),
                (1, "h"),
            ],
        ),
        (["a", "b", "c", "d"], 4, 1, True, [(0, "a"), (1, "b"), (2, "c"), (3, "d")]),
        (["a", "b", "c", "d"], 8, 1, True, [(0, "a"), (2, "b"), (4, "c"), (6, "d")]),
        (["a", "b", "c", "d"], 1, 8, True, [(0, "a"), (0, "b"), (0, "c"), (0, "d")]),
        (["a"], 1, 1, True, [(0, "a")]),
        (["a"], 2, 1, True, [(0, "a")]),
        ([], 1, 1, True, []),
    ],
)
def test_group_images(
    rc, image_group, n_measurements, measurements_per_run, groups_first, grouped_images
):
    output = rc.group_images(
        image_group, n_measurements, measurements_per_run, groups_first
    )
    assert output == grouped_images


# For testing script sanitisation
@pytest.mark.parametrize(
    "scripts,cleaned_scripts",
    [
        ("some; bash; script;", "some; bash; script;"),
        (";some ;bash ;script", "some; bash; script;"),
        ("some bash script", "some bash script;"),
        ("some -options ;; and -commands ;", "some -options; and -commands;"),
        (";some bash; script \r\n;", "some bash; script \n;"),
        (
            ";some; nightmarish -bash \r\n script ;; \n",
            "some; nightmarish -bash \n script; \n;",
        ),
    ],
)
def test_sanitise_scripts(rc, scripts, cleaned_scripts):
    assert rc.sanitise_scripts(scripts) == cleaned_scripts
